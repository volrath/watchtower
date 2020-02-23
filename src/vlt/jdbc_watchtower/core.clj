(ns vlt.jdbc-watchtower.core
  (:require [clojure.core.async :as a]
            [clojure.set :as set])
  (:import [net.ttddyy.dsproxy ExecutionInfo QueryInfo]
           net.ttddyy.dsproxy.listener.QueryExecutionListener
           net.ttddyy.dsproxy.proxy.ParameterSetOperation
           net.ttddyy.dsproxy.support.ProxyDataSourceBuilder))

(defmulti ->clj
  "Transforms instances of datasource-proxy classes into Clojure data structures.
  This multimethod is meant to be used to transform the parameters passed to a
  `QueryExecutionListener`'s \"before/after query\" functions."
  type)


(defmethod ->clj ExecutionInfo
  [exec-info]
  (-> (bean exec-info)
      (select-keys [:statement :connectionId :success
                    :elapsedTime :batch :batchSize
                    :dataSourceName])
      (set/rename-keys {:batchSize      :batch-size
                        :connectionId   :connection-id
                        :dataSourceName :datasource-name
                        :elapsedTime    :elapsed-time})
      (update :statement (fn [statement]
                           (str (cond-> statement
                                  (instance? java.sql.Wrapper statement)
                                  (.unwrap java.lang.Object)))))))


(defmethod ->clj QueryInfo
  [query-info]
  (let [{query :query parameters-list :parametersList}
        (bean query-info)]
    {:query          query
     :parameter-list (map (fn [parameters]
                            (reduce (fn [acc ^ParameterSetOperation param]
                                      (let [[k v] (.getArgs param)]
                                        (assoc acc
                                               k
                                               {:value  v
                                                :method (str (.getMethod param))})))
                                    {}
                                    parameters))
                          parameters-list)}))


(defmethod ->clj java.util.ArrayList
  [array-list]
  (mapv ->clj array-list))


(defmethod ->clj :default
  [x]
  x)


(defn ^QueryExecutionListener custom-listener
  "Thin wrapper over a reification of `QueryExecutionListener`.
  This function expects a map with `:before-query` and/or `:after-query` keys,
  which should be functions that will be called before/after any query that goes
  through the proxy.

  Both functions receive the same arguments: excecution-info and
  query-info-list. For more information, please refer to:
  - `net.ttddyy.dsproxy.ExecutionInfo`
  - `net.ttddyy.dsproxy.QueryInfo`
  - Their `->clj` multimethod implementations."
  [{:keys [before-query after-query] :as _opts}]
  (reify QueryExecutionListener
    (beforeQuery [_this exec-info query-info-list]
      (when (some? before-query)
        (before-query (->clj exec-info) (->clj query-info-list))))
    (afterQuery  [_this exec-info query-info-list]
      (when (some? after-query)
        (after-query (->clj exec-info) (->clj query-info-list))))))


(defn ^QueryExecutionListener stdout-listener
  "Returns a `QueryExecutionListener` instance that will print queries to stdout."
  []
  (custom-listener
   {:after-query (fn [exec-info _query-info-list]
                   (println)
                   (println (format "[jdbc-watchtower:%s] %s"
                                    (:datasource-name exec-info)
                                    (pr-str (select-keys exec-info [:connection-id :success
                                                                    :batch :batch-size
                                                                    :elapsed-time]))))
                   (println (:statement exec-info)))}))


(defn ^QueryExecutionListener channel-listener
  "Returns a `QueryExecutionListener` instance that will put into `ch` every
  query's 'execution info' and 'query-info-list'."
  [ch]
  (custom-listener
   {:after-query (fn [exec-info query-info-list]
                   (a/put! ch {:execution-info  (->clj exec-info)
                               :query-info-list (->clj query-info-list)}))}))


(defn proxy-datasource
  "Creates a proxy over `datasource` that allows the injection of code before and
  after queries are executed.

  This function is a thin wrapper over
  `net.ttddyy.dsproxy.support.ProxyDataSourceBuilder/create`, which in turn
  returns an instance of `net.ttddyy.dsproxy.support.ProxyDataSource`.

  `ProxyDataSource` instances act as a regular JDBC data source, but they can be
  augmented with a 'listener', which should be an implementation/instance of
  `net.ttddyy.dsproxy.listener.QueryExecutionListener`, and should provide
  functions to be called before and/or after queries are executed.

  This function accepts an optional map which can include:
  - `:datasource-name`: a string to identify the new data source.
  - `:before-query`: a function to be called before queries are executed. This
    function will be called with the `execution-info` and `query-info-list` of
    the query.
  - `:after-query`: a function to be called after queries are executed. This
    function will be called with the `execution-info` and `query-info-list` of
    the query.
  - `:listener`: a custom implementation of `QueryExecutionListener`.

  If `:before-query` or `:after-query` are not nil, the proxy will use a custom
  listener that calls these functions (see
  `vlt.jdbc-watchtower.core/custom-listener` for more info)

  Please notice that `custom-listener` will clojurify the arguments passed to
  `after-query` and `before-query`.  Keep this in mind if you decide to create
  your own implementation of `QueryExecutionListener`, in which case the
  arguments passed to it will be java instances of `ExecutionInfo` and
  `ArrayList<QueryInfo>` respectively.  You can call
  `vlt.jdbc-watchtower.core/->clj` over these objects to get a clojure-y version
  of them.

  If neither `:after-query`, `:before-query`, or `:listener` are defined, this
  function will return a proxy that uses
  `vlt.jdbc-watchtower.core/stdout-listener` as its listener."
  ([datasource]
   (proxy-datasource datasource {}))
  ([datasource {:keys [listener before-query after-query datasource-name]
                :or   {datasource-name "default"
                       listener        (stdout-listener)}}]
   {:pre [(or listener before-query after-query)]}
   (let [listener (if (or (some? before-query)
                          (some? after-query))
                    (custom-listener {:before-query before-query
                                      :after-query  after-query})
                    listener)]
     (-> (ProxyDataSourceBuilder/create datasource)
         (.name datasource-name)
         (.listener ^QueryExecutionListener listener)
         (.build)))))
