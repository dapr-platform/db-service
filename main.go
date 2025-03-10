package main

import (
	"context"
	_ "db-service/prom"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/gosidekick/migration/v3"
	"github.com/hitlyl/prest/adapters/postgres"
	"github.com/hitlyl/prest/config"
	pctx "github.com/hitlyl/prest/context"
	"github.com/hitlyl/prest/controllers"
	"github.com/hitlyl/prest/middlewares"
	"github.com/hitlyl/prest/plugins"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	slog "github.com/structy/log"
	"github.com/urfave/negroni/v3"
	_ "go.uber.org/automaxprocs"
)

var promHandler = promhttp.Handler()

func initCustomRouter() *mux.Router {

	router := mux.NewRouter().StrictSlash(true)
	// if auth is enabled
	if config.PrestConf.AuthEnabled {
		router.HandleFunc("/auth", controllers.Auth).Methods("POST")
	}
	router.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		promHandler.ServeHTTP(w, r)

	})
	router.HandleFunc("/databases", controllers.GetDatabases).Methods("GET")
	router.HandleFunc("/schemas", controllers.GetSchemas).Methods("GET")
	router.HandleFunc("/tables", controllers.GetTables).Methods("GET")
	router.HandleFunc("/health", Health).Methods("GET")

	router.HandleFunc("/migrations/{verb}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		verb := vars["verb"]
		paras := r.URL.Query()
		n := paras["n"]
		vn := verb
		if n != nil {
			vn = verb + " " + n[0]
		}
		_, executed, err := migration.Run(r.Context(), config.PrestConf.MigrationsPath, config.PrestConf.PGURL, vn)
		if err != nil {
			slog.Errorf("run migration error,verb=%s err=%s, executed=%s", verb, err.Error(), executed)
			w.Write([]byte("run migration error " + err.Error()))
			w.WriteHeader(http.StatusInternalServerError)

		} else {
			slog.Debugln("run migration success")
			ret := ""
			for _, s := range executed {
				ret = ret + s + "\n"
			}
			w.Write([]byte(" ok \n" + ret))
		}

	})

	router.HandleFunc("/_QUERIES/{queriesLocation}/{script}", controllers.ExecuteFromScripts)
	// if it is windows it should not register the plugin endpoint
	// we use go plugin system that does not support windows
	// https://github.com/golang/go/issues/19282
	if runtime.GOOS != "windows" {
		router.HandleFunc("/_PLUGIN/{file}/{func}", plugins.HandlerPlugin)
	}
	router.HandleFunc("/{database}/{schema}", controllers.GetTablesByDatabaseAndSchema).Methods("GET")
	router.HandleFunc("/show/{database}/{schema}/{table}", controllers.ShowTable).Methods("GET")

	crudRoutes := mux.NewRouter().PathPrefix("/").Subrouter().StrictSlash(true)
	crudRoutes.HandleFunc("/{database}/{schema}/{table}", controllers.SelectFromTables).Methods("GET")
	crudRoutes.HandleFunc("/{database}/{schema}/{table}", controllers.InsertInTables).Methods("POST")
	crudRoutes.HandleFunc("/batch/{database}/{schema}/{table}", controllers.BatchInsertInTables).Methods("POST")
	crudRoutes.HandleFunc("/upsert/{database}/{schema}/{table}", CustomUpsert).Methods("POST")

	crudRoutes.HandleFunc("/{database}/{schema}/{table}", controllers.DeleteFromTable).Methods("DELETE")
	crudRoutes.HandleFunc("/{database}/{schema}/{table}", controllers.UpdateTable).Methods("PUT", "PATCH")
	router.PathPrefix("/").Handler(negroni.New(
		middlewares.CacheMiddleware(&config.PrestConf.Cache),
		plugins.MiddlewarePlugin(),
		middlewares.SetTimeoutToContext(),
		negroni.Wrap(crudRoutes),
	))

	return router
}

type doneWriter struct {
	http.ResponseWriter
	done   bool
	status int
}

func (w *doneWriter) WriteHeader(status int) {
	w.done = true
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *doneWriter) Write(b []byte) (int, error) {
	w.done = true
	if w.status > 299 {
		slog.Errorln("DB-SERVICE ERROR:", string(b))
	}

	return w.ResponseWriter.Write(b)
}

func CustomUpsert(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	database := vars["database"]
	schema := vars["schema"]
	table := vars["table"]
	keys := r.URL.Query().Get("keys")
	batch := r.URL.Query().Get("batch")
	ignoreKeys := r.URL.Query().Get("ignore_keys")
	var useIgnoreKeys = false
	if ignoreKeys != "" {
		useIgnoreKeys = true
	}
	var script string
	if batch == "true" {
		if useIgnoreKeys {
			script = "queries/upsert/batch_upsert_with_ignore.write.sql"
		} else {
			script = "queries/upsert/batch_upsert.write.sql"
		}

	} else {
		if useIgnoreKeys {
			script = "queries/upsert/upsert_with_ignore.write.sql"

		} else {
			script = "queries/upsert/upsert.write.sql"
		}

	}
	if keys == "" {
		err := fmt.Errorf("upsert keys should be set")
		slog.Debugln(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	arr := strings.Split(keys, ",")
	newKeys := ""
	for _, v := range arr {
		newKeys = newKeys + "'" + v + "',"
	}
	newKeys = strings.TrimSuffix(newKeys, ",")
	if ignoreKeys != "" {
		igArr := strings.Split(ignoreKeys, ",")
		ignoreKeys = ""
		for _, v := range igArr {
			ignoreKeys = ignoreKeys + "'" + v + "',"
		}
		ignoreKeys = strings.TrimSuffix(ignoreKeys, ",")
	}

	if config.PrestConf.SingleDB && (config.PrestConf.Adapter.GetDatabase() != database) {
		err := fmt.Errorf("database not registered: %v", database)
		slog.Errorln(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Errorln("readall body:", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	bodyStr := ""
	if batch == "true" {
		var bodyArr []interface{}
		err := json.Unmarshal(body, &bodyArr)
		if err != nil {
			slog.Errorln("unmarshal body:", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if bodyArr == nil || len(bodyArr) == 0 {
			slog.Errorln("body array is empty")
			http.Error(w, "body should not be empty", http.StatusBadRequest)
			return
		}
		for _, item := range bodyArr {
			m := item.(map[string]interface{})
			for key, value := range m {
				if value == nil {
					delete(m, key)
				}

			}

			itemStr, _ := json.Marshal(item)
			bodyStr = bodyStr + "'" + string(itemStr) + "'::json,"
		}
		if bodyStr == "" {
			slog.Errorln("body array is empty")
			http.Error(w, "body should not be empty", http.StatusBadRequest)
			return
		}
		bodyStr = strings.TrimSuffix(bodyStr, ",")
		bodyStr = "array[" + bodyStr + "]"
	} else {
		bodyStr = "'" + string(body) + "'::json"
	}

	config.PrestConf.Adapter.SetDatabase(config.PrestConf.PGDatabase)
	templateData := make(map[string]interface{})
	templateData["schema"] = "'" + schema + "'"
	templateData["table"] = "'" + table + "'"
	templateData["keys"] = "array[" + newKeys + "]"
	if useIgnoreKeys {
		templateData["ignore_keys"] = "array[" + ignoreKeys + "]"
	}

	templateData["values"] = bodyStr

	sql, values, err := config.PrestConf.Adapter.ParseScript(script, templateData)

	if err != nil {
		err = fmt.Errorf("could not parse script %s, %+v", script, err)
		slog.Errorln(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// set db name on ctx
	ctx := context.WithValue(r.Context(), pctx.DBNameKey, database)

	timeout, _ := ctx.Value(pctx.HTTPTimeoutKey).(int)
	ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(timeout))
	defer cancel()

	sc := config.PrestConf.Adapter.ExecuteScriptsCtx(ctx, "POST", sql, values)
	//sc := config.PrestConf.Adapter.ExecuteScripts("POST", sql, values)
	if err1 := sc.Err(); err1 != nil {
		errstr := fmt.Errorf("could not execute sql %s, %s", sql, err1.Error())
		slog.Errorln(errstr)
		http.Error(w, "upsert sql error "+err1.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(sc.Bytes())
}

func main() {
	config.Load()
	if config.PrestConf.Debug {
		slog.DebugMode = true
	}
	slog.Println("config.PrestConf.Debug", config.PrestConf.Debug)
	slog.Println("config.PrestConf.PGURL", config.PrestConf.PGURL)
	slog.Println("config.PrestConf.PGDatabase", config.PrestConf.PGDatabase)
	slog.Println("config.PrestConf.PGUser", config.PrestConf.PGUser)
	slog.Println("config.PrestConf.PGPassword", config.PrestConf.PGPass)
	slog.Println("config.PrestConf.PGHost", config.PrestConf.PGHost)
	slog.Println("config.PrestConf.PGPort", config.PrestConf.PGPort)
	// Load Postgres Adapter
	postgres.Load()

	// Get pREST app
	n := middlewares.GetApp()

	// Get pPREST router
	r := initCustomRouter()
	n.UseHandler(r)
	srv := &http.Server{
		Handler: r,
		Addr:    ":80",
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: time.Duration(config.PrestConf.HTTPTimeout) * time.Second,
		ReadTimeout:  time.Duration(config.PrestConf.HTTPTimeout) * time.Second,
	}

	slog.Fatal(srv.ListenAndServe())

}

func Health(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}
