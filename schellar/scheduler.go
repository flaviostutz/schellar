package main

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/robfig/cron"
	"gopkg.in/mgo.v2/bson"
)

var (
	scheduledRoutines = make(map[string]*cron.Cron)
)

func startScheduler() error {
	err := prepareTimers()
	if err != nil {
		return err
	}
	go checkRunningWorkflows()
	return nil
}

func prepareTimers() error {
	logrus.Debugf("Refreshing timers according to active schedules")

	sc := mongoSession.Copy()
	defer sc.Close()
	st := sc.DB(dbName).C("schedules")

	// var schedules []map[string]interface{}
	var activeSchedules []Schedule
	err := st.Find(bson.M{"enabled": true}).All(&activeSchedules)
	if err != nil {
		return err
	}

	//activate go routines for schedules that weren't activated yet
	for _, activeSchedule := range activeSchedules {
		isScheduled := false
		for idRoutine := range scheduledRoutines {
			if activeSchedule.ID.Hex() == idRoutine {
				isScheduled = true
				// break
			}
		}
		if !isScheduled {
			logrus.Infof("Starting timer for schedule '%s' (%s)", activeSchedule.ID.Hex(), activeSchedule.Name)
			err := launchSchedule(activeSchedule.ID.Hex())
			if err != nil {
				return err
			}
		}
	}

	//remove go routines that are not currenctly active
	for scheduleID, cronJob := range scheduledRoutines {
		isActive := false
		for _, activeSchedule := range activeSchedules {
			if scheduleID == activeSchedule.ID.Hex() {
				isActive = true
				break
			}
		}
		if !isActive {
			logrus.Infof("Stopping timer for schedule id %s", scheduleID)
			cronJob.Stop()
		}
	}

	return nil
}

func launchSchedule(scheduleID string) error {
	logrus.Debugf("launchSchedule=%s", scheduleID)

	sc := mongoSession.Copy()
	defer sc.Close()

	var schedule0 Schedule
	st := sc.DB(dbName).C("schedules")

	// if true {
	// 	var tests []Schedule
	// 	err0 := st.Find(nil).All(&tests)
	// 	if err0 != nil {
	// 		logrus.Errorf(">>>>> ERR111 %s", err0)
	// 	}
	// 	logrus.Infof(">>>>> TEST %v", tests)
	// }

	fid := bson.ObjectIdHex(scheduleID)
	err := st.FindId(fid).One(&schedule0)
	if err != nil {
		return err
	}

	c := cron.New()
	c.AddFunc(schedule0.CronString, func() {
		logrus.Debugf("Processing timer trigger for schedule %s", scheduleID)
		sc := mongoSession.Copy()
		defer sc.Close()

		var schedule Schedule
		st := sc.DB(dbName).C("schedules")
		fid := bson.ObjectIdHex(scheduleID)
		err := st.FindId(fid).One(&schedule)
		if err != nil {
			logrus.Errorf("Couldn't get schedule %s. err=%s", scheduleID, err)
			return
		}

		isBefore := false
		if schedule.ToDate == nil || time.Now().Before(*schedule.ToDate) {
			isBefore = true
		}
		isAfter := false
		if schedule.FromDate == nil || time.Now().After(*schedule.FromDate) {
			isAfter = true
		}
		if isBefore && isAfter {
			logrus.Infof("%s Launching new workflow for schedule '%s'", time.Now(), scheduleID)
			startTime := time.Now()
			workflowID, err := launchWorkflow(scheduleID)
			if err != nil {
				logrus.Errorf("Error launching Workflow err=%s", err)
				return
			}

			logrus.Debugf("Saving workflow run %s", workflowID)
			var scheduleRun ScheduleRun
			scheduleRun.WorkflowID = workflowID
			scheduleRun.StartDate = startTime
			scheduleRun.Status = "RUNNING"
			scheduleRun.LastUpdate = time.Now()
			runID, err0 := saveScheduleRun(scheduleRun)
			if err0 != nil {
				logrus.Errorf("Error saving Schedule Run err=%s", err0)
			}
			logrus.Debugf("Schedule Run saved. id=%s", runID)

			logrus.Debugf("Updating Schedule status. id=%s. status=%s", scheduleRun.ScheduleID, "RUNNING")
			statusMap := make(map[string]interface{})
			statusMap["status"] = "RUNNING"
			statusMap["lastUpdate"] = time.Now()

			sr := sc.DB(dbName).C("runs")
			err0 = sr.UpdateId(scheduleRun.ScheduleID, statusMap)
			if err0 != nil {
				logrus.Errorf("Error saving Schedule status err=%s", err0)
			}

		} else {
			logrus.Debugf("Schedule %s active, but not within activation date", scheduleID)
		}

	})
	scheduledRoutines[scheduleID] = c
	go c.Start()
	return nil
}

func checkRunningWorkflows() {
	logrus.Debugf("Starting to check running workflow status")
	for {
		logrus.Debugf("Checking running workflows...")
		sc := mongoSession.Copy()
		sch := sc.DB(dbName).C("runs")

		scheduleRuns := make([]ScheduleRun, 0)
		err0 := sch.Find(bson.M{"status": "RUNNING"}).All(&scheduleRuns)
		if err0 != nil {
			logrus.Errorf("Error listing RUNNING runs. err=%s", err0)
		} else {
			for _, scheduleRun := range scheduleRuns {
				logrus.Debugf("Checking workflowId %s", scheduleRun.WorkflowID)
				err1 := checkAndUpdateScheduleRunStatus(scheduleRun)
				if err1 != nil {
					logrus.Errorf("Error checking workflow %s. err=%s", scheduleRun.WorkflowID, err1)
				}
			}
		}
		sc.Close()
		time.Sleep(1 * time.Second)
	}
}

func checkAndUpdateScheduleRunStatus(scheduleRun ScheduleRun) error {
	logrus.Debugf("checkAndUpdateScheduleRunStatus id=%s wfid=%s", scheduleRun.ID, scheduleRun.WorkflowID)
	wf, err := getWorkflowRun(scheduleRun.WorkflowID)
	if err != nil {
		return err
	}
	status := wf["status"].(string)
	if status != "RUNNING" {
		logrus.Infof("Workflow %s finished. status=%s", scheduleRun.WorkflowID, status)

		sc := mongoSession.Copy()
		defer sc.Close()
		st := sc.DB(dbName).C("runs")

		logrus.Debugf("Updating ScheduleRun status. id=%s. status=%s", scheduleRun.ID, status)
		scheduleRun.Status = status
		scheduleRun.FinishDate = time.Now()
		scheduleRun.WorkflowDetails = wf
		scheduleRun.LastUpdate = time.Now()
		err0 := st.UpdateId(scheduleRun.ID, scheduleRun)
		if err0 != nil {
			return err0
		}

		logrus.Debugf("Updating Schedule status. id=%s. status=%s", scheduleRun.ScheduleID, status)
		statusMap := make(map[string]interface{})
		statusMap["status"] = status
		statusMap["lastUpdate"] = time.Now()

		if status == "COMPLETED" {
			logrus.Debugf("Updating input/output context")
			sch := sc.DB(dbName).C("schedules")
			var schedule map[string]interface{}
			fid := bson.ObjectIdHex(scheduleRun.ScheduleID)
			err := sch.FindId(fid).One(&schedule)
			if err != nil {
				return err
			}
			currentContext, existsCurrent := schedule["workflowContext"].(map[string]string)
			if !existsCurrent {
				currentContext = make(map[string]string)
			}
			wout, existsWf := scheduleRun.WorkflowDetails["output"].(map[string]string)
			if existsWf {
				for k, v := range wout {
					currentContext[k] = v
				}
			}
			statusMap["workflowContext"] = currentContext
		}
		err0 = st.UpdateId(scheduleRun.ScheduleID, statusMap)
		if err0 != nil {
			return err0
		}

	} else {
		logrus.Debugf("Workflow %s still running", scheduleRun.WorkflowID)
	}
	return nil
}

func getStringValue(m map[string]interface{}, keyName string, defaultValue string) string {
	v, exists := m[keyName]
	if !exists {
		return defaultValue
	}
	return v.(string)
}

func saveScheduleRun(scheduleRun ScheduleRun) (string, error) {
	logrus.Debugf("createScheduleRun r=%s", scheduleRun)
	sc := mongoSession.Copy()
	defer sc.Close()

	sch := sc.DB(dbName).C("runs")
	scheduleRun.ID = bson.NewObjectId()
	err0 := sch.Insert(scheduleRun)
	if err0 != nil {
		return "", err0
	}
	return scheduleRun.ID.Hex(), nil
}
