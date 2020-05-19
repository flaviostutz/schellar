package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"encoding/json"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	mongoSession *mgo.Session
)

func startRestAPI() error {
	router := mux.NewRouter()

	router.Use(customCorsMiddleware)
	router.HandleFunc("/schedule", createSchedule).Methods("POST", "OPTIONS")
	router.HandleFunc("/schedule", listSchedules).Methods("GET")
	router.HandleFunc("/schedule/{name}", getSchedule).Methods("GET")
	router.HandleFunc("/schedule/{name}", deleteSchedule).Methods("DELETE")
	router.HandleFunc("/schedule/{name}", updateSchedule).Methods("PUT", "OPTIONS")
	router.Handle("/metrics", promhttp.Handler())
	listen := fmt.Sprintf("0.0.0.0:3000")
	logrus.Infof("Listening at %s", listen)
	err := http.ListenAndServe(listen, router)
	if err != nil {
		logrus.Errorf("Error while listening requests: %s", err)
		os.Exit(1)
	}
	return nil
}

func createSchedule(w http.ResponseWriter, r *http.Request) {
	logrus.Debugf("createSchedule r=%v", r)

	decoder := json.NewDecoder(r.Body)
	// schedule := make(map[string]interface{})
	var schedule Schedule
	err := decoder.Decode(&schedule)
	if err != nil {
		writeResponse(w, http.StatusBadRequest, fmt.Sprintf("Error handling post results. err=%s", err.Error()))
		return
	}
	if schedule.Name == "" {
		writeResponse(w, http.StatusBadRequest, "'name' is required")
		return
	}
	if schedule.WorkflowName == "" {
		writeResponse(w, http.StatusBadRequest, "'workflowName' is required")
		return
	}
	if schedule.CronString == "" {
		writeResponse(w, http.StatusBadRequest, "'cronString' is required")
		return
	}
	if len(strings.Split(schedule.CronString, " ")) != 6 {
		writeResponse(w, http.StatusBadRequest, "'cronString' is invalid. It must have 5 spaces")
		return
	}
	if schedule.WorkflowVersion == "" {
		schedule.WorkflowVersion = "1"
	}
	// if !schedule.Enabled {
	// 	schedule.Enabled = true
	// }
	if schedule.CheckWarningSeconds == 0 {
		schedule.CheckWarningSeconds = 3600
	}
	schedule.LastUpdate = time.Now()

	// _, err1 := getWorkflow(schedule.WorkflowName, schedule.WorkflowVersion)
	// if err1 != nil {
	// 	writeResponse(w, http.StatusBadRequest, fmt.Sprintf("Workflow '%s' %s doesn't exist in Conductor", schedule.WorkflowName, schedule.WorkflowVersion))
	// 	return
	// }
	// logrus.Debugf("Workflow %s exists in Conductor", schedule.WorkflowName)

	sc := mongoSession.Copy()
	defer sc.Close()
	st := sc.DB(dbName).C("schedules")

	//check duplicate schedule
	c, err1 := st.Find(bson.M{"name": schedule.Name}).Count()
	if err1 != nil {
		writeResponse(w, http.StatusInternalServerError, "Error checking for existing schedule name")
		logrus.Errorf("Error checking for existing schedule name. err=%s", err1)
		return
	}
	if c > 0 {
		writeResponse(w, http.StatusBadRequest, fmt.Sprintf("Duplicate schedule name '%s'", schedule.Name))
		return
	}

	logrus.Debugf("Saving schedule %s for workflow %s", schedule.Name, schedule.WorkflowName)
	logrus.Debugf("schedule: %v", schedule)
	err0 := st.Insert(schedule)
	if err0 != nil {
		writeResponse(w, http.StatusInternalServerError, "Error storing schedule.")
		logrus.Errorf("Error storing schedule to Mongo. err=%s", err0)
		return
	}
	prepareTimers()
	logrus.Debugf("Sending response")
	w.Header().Set("Content-Type", "plain/text")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusCreated)
}

func updateSchedule(w http.ResponseWriter, r *http.Request) {
	logrus.Debugf("updateSchedule r=%v", r)
	name := mux.Vars(r)["name"]

	decoder := json.NewDecoder(r.Body)
	schedule2 := make(map[string]interface{})
	err := decoder.Decode(&schedule2)
	if err != nil {
		writeResponse(w, http.StatusBadRequest, fmt.Sprintf("Error updating schedule. err=%s", err.Error()))
		return
	}

	sc := mongoSession.Copy()
	defer sc.Close()
	st := sc.DB(dbName).C("schedules")

	logrus.Debugf("Updating schedule with %v", schedule2)
	c, err1 := st.Find(bson.M{"name": name}).Count()
	if err1 != nil {
		writeResponse(w, http.StatusInternalServerError, fmt.Sprintf("Error updating schedule"))
		logrus.Errorf("Couldn't find schedule name %s. err=%s", name, err1)
		return
	}
	if c == 0 {
		writeResponse(w, http.StatusNotFound, fmt.Sprintf("Couldn't find schedule %s", name))
		return
	}
	err = st.Update(bson.M{"name": name}, bson.M{"$set": schedule2})
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, "Error updating schedule")
		logrus.Errorf("Error updating schedule %s. err=%s", name, err)
		return
	}
	prepareTimers()
	writeResponse(w, http.StatusOK, fmt.Sprintf("Schedule updated successfully"))
}

func listSchedules(w http.ResponseWriter, r *http.Request) {
	logrus.Debugf("listSchedules r=%v", r)

	sc := mongoSession.Copy()
	defer sc.Close()
	st := sc.DB(dbName).C("schedules")

	// var schedules []map[string]interface{}
	schedules := make([]Schedule, 0)
	err := st.Find(nil).All(&schedules)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, fmt.Sprintf("Error listing schedules. err=%s", err.Error()))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	logrus.Debugf("Schedules=%v", schedules)
	b, err0 := json.Marshal(schedules)
	if err0 != nil {
		writeResponse(w, http.StatusInternalServerError, fmt.Sprintf("Error listing schedules. err=%s", err.Error()))
		return
	}
	w.Write(b)
	logrus.Debugf("result: %s", string(b))
}

func getSchedule(w http.ResponseWriter, r *http.Request) {
	logrus.Debugf("getSchedule r=%v", r)
	name := mux.Vars(r)["name"]

	sc := mongoSession.Copy()
	defer sc.Close()
	st := sc.DB(dbName).C("schedules")

	// var schedule map[string]interface{}
	var schedule Schedule
	err := st.Find(bson.M{"name": name}).One(&schedule)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, fmt.Sprintf("Error getting schedule. err=%s", err.Error()))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	b, err0 := json.Marshal(schedule)
	if err0 != nil {
		writeResponse(w, http.StatusInternalServerError, fmt.Sprintf("Error getting schedule. err=%s", err.Error()))
		return
	}
	w.Write(b)
	logrus.Debugf("result: %s", string(b))
}

func deleteSchedule(w http.ResponseWriter, r *http.Request) {
	logrus.Debugf("deleteSchedule r=%v", r)
	name := mux.Vars(r)["name"]

	sc := mongoSession.Copy()
	defer sc.Close()
	st := sc.DB(dbName).C("schedules")

	err := st.Remove(bson.M{"name": name})
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, fmt.Sprintf("Error deleting schedule. err=%s", err.Error()))
		return
	}
	prepareTimers()
	writeResponse(w, http.StatusOK, fmt.Sprintf("Deleted schedule successfully. name=%s", name))
}

func writeResponse(w http.ResponseWriter, statusCode int, message string) {
	msg := make(map[string]string)
	msg["message"] = message
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(msg)
}

func customCorsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "POST, GET, PUT, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Requested-With")
			return
		}
		next.ServeHTTP(w, r)
	})
}
