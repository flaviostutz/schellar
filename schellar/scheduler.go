package main

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/robfig/cron"
	"gopkg.in/mgo.v2/bson"
)

var (
	scheduledRoutines map[string]*cron.Cron
)

func startScheduler() {
	launchSchedules()
	go checkRunningWorkflows()
}

func launchSchedules() error {
	logrus.Debugf("launchSchedules sync Go routines with schedules from database")

	sc := mongoSession.Copy()
	defer sc.Close()
	st := sc.DB(dbName).C("schedules")

	// var schedules []map[string]interface{}
	var schedules []Schedule
	err := st.Find(nil).Select(bson.M{"active": true}).All(&schedules)
	if err != nil {
		return err
	}

	activeSchedules := make(map[string]Schedule)
	for _, schedule := range schedules {
		activeSchedules[schedule.ID.Hex()] = schedule
	}

	//activate go routines for schedules that weren't activated yet
	for idActive, schedule := range activeSchedules {
		isScheduled := false
		for idRoutine := range scheduledRoutines {
			if idActive == idRoutine {
				isScheduled = true
				break
			}
		}
		if !isScheduled {
			logrus.Infof("Starting timer for schedule '%s'", schedule.Name)
			err := launchSchedule(schedule)
			if err != nil {
				return err
			}
		}
	}

	//remove go routines that are not currenctly active
	for scheduleID, cronJob := range scheduledRoutines {
		isActive := false
		for activeID := range activeSchedules {
			if scheduleID == activeID {
				isActive = true
				break
			}
		}
		if !isActive {
			logrus.Infof("")
			logrus.Infof("Stopping timer for schedule id %s", scheduleID)
			cronJob.Stop()
		}
	}

	return nil
}

func launchSchedule(schedule Schedule) error {
	logrus.Debugf("schedule=%s", schedule)

	scheduleID := schedule.ID.Hex()
	c := cron.New()
	//trigger new workflows
	c.AddFunc(schedule.CronString, func() {
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
		sc := mongoSession.Copy()
		defer sc.Close()
		sch := sc.DB(dbName).C("runs")
		statusMap := make(map[string]interface{})
		statusMap["status"] = "RUNNING"
		statusMap["lastUpdate"] = time.Now()
		err0 = sch.UpdateId(scheduleRun.ScheduleID, statusMap)
		if err0 != nil {
			logrus.Errorf("Error saving Schedule status err=%s", err0)
		}
	})
	scheduledRoutines[scheduleID] = c
	go c.Start()
	return nil
}

func checkRunningWorkflows() {
	logrus.Debugf("Starting routine that will check for running workflows status")
	for {
		sc := mongoSession.Copy()
		sch := sc.DB(dbName).C("runs")

		scheduleRuns := make([]ScheduleRun, 0)
		err0 := sch.Find(nil).Select(bson.M{"status": "RUNNING"}).All(&scheduleRuns)
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
			err := sch.FindId(scheduleRun.ScheduleID).One(&schedule)
			if err != nil {
				return err
			}
			currentContext, existsCurrent := schedule["currentContext"].(map[string]string)
			if !existsCurrent {
				currentContext = make(map[string]string)
			}
			wout, existsWf := scheduleRun.WorkflowDetails["output"].(map[string]string)
			if existsWf {
				for k, v := range wout {
					currentContext[k] = v
				}
			}
			err = sch.UpdateId(scheduleRun.ScheduleID, currentContext)
			if err != nil {
				return err
			}
			logrus.Debugf("Context updated successfully")
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
	ns, err0 := sch.UpsertId(bson.M{"nonsense": -1}, scheduleRun)
	if err0 != nil {
		return "", err0
	}
	uid := ns.UpsertedId.(bson.ObjectId)
	return uid.Hex(), nil
}
