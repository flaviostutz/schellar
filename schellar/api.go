package main

import (
	"fmt"
	"net/http"
	"os"

	"encoding/json"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	mongoSession *mgo.Session
)

func startRestAPI() error {
	router := mux.NewRouter()
	router.HandleFunc("/schedule", createSchedule).Methods("POST")
	router.HandleFunc("/schedule", listSchedules).Methods("GET")
	router.HandleFunc("/schedule/{id}", getSchedule).Methods("GET")
	router.HandleFunc("/schedule/{id}", deleteSchedule).Methods("DELETE")
	router.HandleFunc("/schedule/{id}", updateSchedule).Methods("PUT")
	// router.HandleFunc("/schedule/{id}/run", createScheduleRun).Methods("POST")
	router.HandleFunc("/schedule/{id}/run", listScheduleRun).Methods("GET")
	// router.HandleFunc("/schedule/{id}/run/{rid}", updateScheduleRun).Methods("PUT")
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

func listScheduleRun(w http.ResponseWriter, r *http.Request) {
	logrus.Debugf("listScheduleRun r=%v", r)
	id := r.URL.Query().Get("id")

	sc := mongoSession.Copy()
	defer sc.Close()
	st := sc.DB(dbName).C("runs")
	// schedules := make([]map[string]interface{}, 0)
	var scheduleRuns []ScheduleRun
	err0 := st.Find(nil).Select(bson.M{"scheduleId": id}).All(&scheduleRuns)
	if err0 != nil {
		writeResponse(w, http.StatusInternalServerError, fmt.Sprintf("Error listing schedule runs for %s. err=%s", id, err0.Error()))
		return
	}

	bschedulesruns, err0 := json.Marshal(scheduleRuns)
	if err0 != nil {
		writeResponse(w, http.StatusInternalServerError, fmt.Sprintf("Error listing schedule runs for %s. err=%s", id, err0.Error()))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	w.Write(bschedulesruns)
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

	_, err1 := getWorkflow(schedule.WorkflowVersion, schedule.WorkflowVersion)
	if err1 != nil {
		writeResponse(w, http.StatusBadRequest, fmt.Sprintf("Workflow '%s' %s doesn't exist in Conductor", schedule.WorkflowName, schedule.WorkflowVersion))
		return
	}

	sc := mongoSession.Copy()
	defer sc.Close()
	st := sc.DB(dbName).C("schedules")
	schedule.CurrentContext = schedule.InitialContext
	ns, err0 := st.Upsert(bson.M{"nonsense": -1}, schedule)
	if err0 != nil {
		writeResponse(w, http.StatusInternalServerError, fmt.Sprintf("Error storing schedule. err=%s", err.Error()))
		return
	}
	w.Header().Set("Content-Type", "plain/text")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusCreated)
	uid := ns.UpsertedId.(bson.ObjectId)
	w.Write([]byte(uid.Hex()))
}

func updateSchedule(w http.ResponseWriter, r *http.Request) {
	logrus.Debugf("updateSchedule r=%v", r)
	id := r.URL.Query().Get("id")

	decoder := json.NewDecoder(r.Body)
	// schedule := make(map[string]interface{})
	var schedule Schedule
	err := decoder.Decode(&schedule)
	if err != nil {
		writeResponse(w, http.StatusBadRequest, fmt.Sprintf("Error updating schedule. err=%s", err.Error()))
		return
	}

	sc := mongoSession.Copy()
	defer sc.Close()
	st := sc.DB(dbName).C("schedules")
	err = st.UpdateId(id, schedule)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, fmt.Sprintf("Error updating schedule. err=%s", err.Error()))
		return
	}
	writeResponse(w, http.StatusCreated, fmt.Sprintf("Schedule updated successfully"))
}

func listSchedules(w http.ResponseWriter, r *http.Request) {
	logrus.Debugf("listSchedules r=%v", r)

	sc := mongoSession.Copy()
	defer sc.Close()
	st := sc.DB(dbName).C("schedules")

	// var schedules []map[string]interface{}
	var schedules []Schedule
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
	id := r.URL.Query().Get("id")

	sc := mongoSession.Copy()
	defer sc.Close()
	st := sc.DB(dbName).C("schedules")

	// var schedule map[string]interface{}
	var schedule Schedule
	err := st.FindId(id).One(&schedule)
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
	id := r.URL.Query().Get("id")

	sc := mongoSession.Copy()
	defer sc.Close()
	st := sc.DB(dbName).C("schedules")

	err := st.RemoveId(id)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, fmt.Sprintf("Error deleting schedule. err=%s", err.Error()))
		return
	}
	writeResponse(w, http.StatusOK, fmt.Sprintf("Deleted schedule successfully. id=%s", id))
}

func writeResponse(w http.ResponseWriter, statusCode int, message string) {
	msg := make(map[string]string)
	msg["message"] = message
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(msg)
}
