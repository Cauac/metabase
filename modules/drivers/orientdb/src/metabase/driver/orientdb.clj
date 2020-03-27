(ns metabase.driver.orientdb
  (:require [clojure.tools.logging :as log]
            [metabase.driver :as driver]
            [metabase.util.honeysql-extensions :as hx]
            [metabase.models.table :refer [Table]]
            [metabase.models.field :refer [Field]]
            [metabase.query-processor.store :as store]
            [metabase.driver.sql-jdbc.sync :as sync]
            [metabase.driver.sql-jdbc.connection :as jdbc-conn]
            [metabase.driver.sql.query-processor :as sql.qp]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str])
  (:import (java.util Date)))

(defmethod jdbc-conn/connection-details->spec :orientdb [_ {:keys [host dbname user password]}]
  {:classname   "com.orientechnologies.orient.jdbc.OrientJdbcDriver"
   :subprotocol "orient"
   :subname     (str "remote:" host "/" dbname)
   :user        user
   :password    password})

(defmethod driver/execute-query :orientdb [_ query]
  (let [db-connection (jdbc-conn/db->pooled-connection-spec (store/database))
        query-str (get-in query [:native :query])
        results (jdbc/query db-connection [query-str])
        columns (map name (keys (first results)))
        rows (map vals results)]
    {:columns columns
     :rows    rows}))

(def column-types-map {:BOOLEAN      :type/Boolean
                       :INTEGER      :type/Integer
                       :SHORT        :type/Integer
                       :LONG         :type/BigInteger
                       :FLOAT        :type/Float
                       :DOUBLE       :type/Float
                       :DATETIME     :type/DateTime
                       :STRING       :type/Text
                       :BINARY       :type/Text
                       :EMBEDDED     :type/Text
                       :EMBEDDEDLIST :type/Text
                       :EMBEDDEDSET  :type/Text
                       :EMBEDDEDMAP  :type/Dictionary
                       :LINK         :type/Text
                       :LINKLIST     :type/Text
                       :LINKSET      :type/Text
                       :LINKMAP      :type/Text
                       :BYTE         :type/Text
                       :TRANSIENT    :type/Text
                       :DATE         :type/Date
                       :CUSTOM       :type/Text
                       :DECIMAL      :type/Decimal
                       :LINKBAG      :type/Text
                       :ANY          :type/Text})

(defmethod sync/database-type->base-type :orientdb [_ database-type]
  (column-types-map database-type))

(defmethod driver/mbql->native :orientdb [d q]
  (log/warn "Q:" q)
  (let [r (sql.qp/mbql->native d q)]
    (log/warn "R:" r)
    r))

(defmethod sql.qp/quote-style :orientdb [_] :mysql)

(defmethod sql.qp/->honeysql [:sql (class Table)] [_ table]
  (let [{table-name :name} table]
    (hx/qualify-and-escape-dots table-name)))
;
;(defmethod sql.qp/->honeysql [:orientdb String] [_ s]
;  (sql.qp/->honeysql :presto s))
;
;(defmethod sql.qp/->honeysql [:orientdb Boolean] [_ bool]
;  (sql.qp/->honeysql :presto bool))

;(defmethod sql.qp/->honeysql [:presto Date] [_ date]
;  (sql.qp/->honeysql :presto date))

(defmethod sql.qp/->honeysql [:orientdb (class Field)] [driver field]
  (let [field-identifier (keyword (:name field))]
    (sql.qp/cast-unix-timestamp-field-if-needed driver field field-identifier)))

(defn date-format [expr format]
  (honeysql.core/raw (str (name expr) ".format('" format "')")))

(defmethod sql.qp/date [:orientdb :minute] [_ _ d] (date-format d "yyyy-MM-dd HH:mm"))
(defmethod sql.qp/date [:orientdb :hour] [_ _ d] (date-format d "yyyy-MM-dd HH"))
(defmethod sql.qp/date [:orientdb :day] [_ _ d] (date-format d "yyyy-MM-dd"))
(defmethod sql.qp/date [:orientdb :month] [_ _ d] (date-format d "yyyy-MM"))
(defmethod sql.qp/date [:orientdb :week] [_ _ d] (date-format d "yyyy-ww"))
(defmethod sql.qp/date [:orientdb :year] [_ _ d] (date-format d "yyyy"))
(defmethod sql.qp/date [:orientdb :minute-of-hour] [_ _ d] (date-format d "mm"))
(defmethod sql.qp/date [:orientdb :hour-of-day]    [_ _ d] (date-format d "HH"))
(defmethod sql.qp/date [:orientdb :day-of-week]   [_ _ d] (date-format d "u"))
(defmethod sql.qp/date [:orientdb :day-of-month]   [_ _ d] (date-format d "dd"))
(defmethod sql.qp/date [:orientdb :day-of-year]   [_ _ d] (date-format d "DDD"))
(defmethod sql.qp/date [:orientdb :month-of-year]  [_ _ d] (date-format d "MM"))
(defmethod sql.qp/date [:orientdb :week-of-year]  [_ _ d] (date-format d "ww"))



