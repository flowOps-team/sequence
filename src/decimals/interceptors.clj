(ns decimals.interceptors
  (:require [io.pedestal.http :as http]
            [io.pedestal.http.route :as route]
            [io.pedestal.interceptor.chain :as chain]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clojure.walk]
            [spec-tools.core :as st]
            [clojure.spec.alpha :as s]
            [decimals.security :as sec]
            [decimals.crypto :as crypto]
            [decimals.transactions :as tx]
            [decimals.analytics :as analytics]
            [decimals.analytics.transactions :as analytics.transactions]
            [decimals.balances :as b]))

(def msg
  {:generic { :message "Sorry, we had a problem. Please, try again or reach out."}
   :funds { :message "Insufficient funds, check origin account balance."}
   :creds { :message "Invalid credentials."}})

(defn response [status body & {:as headers}]
  {:status status :body body :headers headers})

(def ok       (partial response 200))
(def created  (partial response 201))
(def accepted (partial response 202))
(def badrequest (partial response 400))
(def forbidden (partial response 401))
(def not-found (partial response 404))
(def server-error (partial response 500))

(defn req-meta [context]
  (select-keys (:request context) [:path-info
                                   :remote-addr
                                   :request-method
                                   :server-name
                                   :transit-params
                                   :uri
                                   ]))

(defn respond [context response]
  (let [m (req-meta context)]
    (case (:status response)
      200 (analytics/track context "Success" m)
      201 (analytics/track context "Created" m)
      400 (analytics/track context "Bad request" m)
      401 (analytics/track context "Unauthorized" m)
      500 (analytics/track context "Server Error" m)))
  (chain/terminate (assoc context :response response)))

(def genesis-balance
  {:name :genesis-balance
   :enter
   (fn [context]
     (if-let [genesis (b/genesis context)]
       (assoc-in context [:from :balance] genesis)
       context))})

(def check-balance
  {:name :check-balance
   :enter
   (fn [context]
     (log/debug "checking origin funds " (get-in context [:from :balance :balance]) (get-in context [:tx :amount]))
     (if-let [balance (get-in context [:from :balance :balance])]
       (when-let [amount (get-in context [:tx :amount])]
         (if-not (>= balance amount)
           (respond context (badrequest (:funds msg)))
           context))
       (respond context (badrequest (:funds msg)))))})

(def hash-txs
  {:name :hash-txs
   :enter
   (fn [context]
     (if-let [ctx (tx/hash-txs context)]
       ctx
       (respond context (server-error {:message (:generic msg)}))))})

(def chain-txs
  {:name :chain-tx
   :enter
   (fn [context]
     (let [r (tx/chain context)]
       (if-let [reply (:success r)]
         (respond context (created (map #(st/select-spec ::tx/pub-transaction %) reply)))
         (respond context (server-error {:message (:generic msg)})))))})

(def spec-tx
  {:name :spec-tx
   :enter
   (fn [context]
     (if-not (s/valid? ::tx/transaction (:tx context))
       (respond context (badrequest {:error (s/explain-data ::tx/transaction (:tx context))}))
       context))})

(defn str->map [str]
  (try
    (-> str json/read-str clojure.walk/keywordize-keys)
    (catch Exception e)))

(def parse-tx
  {:name :parse-tx
   :enter
   (fn [context]
     (if-let [tx (str->map (slurp (:body (:request context))))]
       (assoc context :tx tx)
       (respond context (badrequest {:error "Malformed JSON."}))))})

(def account-queryparam
  {:name :account-queryparam
   :enter
   (fn [context]
     (if-let [accounts (get-in context [:request :query-params :account])]
       (let [route-name (get-in context [:route :route-name])]
         (case route-name
           ;; Multiple accounts for stats and trends
           (:transactions-stats :transactions-trends)
           (let [account-list (-> accounts
                                 (clojure.string/split #",")
                                 distinct)
                 _ (when (> (count account-list) 100000)
                     (respond context (badrequest {:error "Maximum 100,000 accounts allowed"})))
                 ids (map #(b/ctx->id (assoc-in context [:request :query-params :account] %))
                         account-list)]
             (log/debug "Querying multiple accounts:" ids)
             (assoc context :accounts ids))
           
           ;; Single account for other routes
           (let [id (b/ctx->id (assoc-in context [:request :query-params :account] accounts))]
             (log/debug "Querying single account:" id)
             (assoc context :account id))))
       ;; Default to public key when no account specified
       (let [pk (get-in context [:customer :public-key])]
         (log/debug "Querying public-key" pk)
         (if (#{:transactions-stats :transactions-trends} (get-in context [:route :route-name]))
           (assoc context :accounts [{:public-key pk}])
           (assoc context :account {:public-key pk})))))})

(def transaction-query-params
  {:name :transaction-query-params
   :enter
   (fn [context]
     (let [params (get-in context [:request :query-params])
           limit (when-let [l (:limit params)]
                  (try (Integer/parseInt l)
                       (catch Exception _ nil)))
           query-params (cond-> {}
                         limit (assoc :limit limit)
                         (:start_date params) (assoc :start-date (:start_date params))
                         (:end_date params) (assoc :end-date (:end_date params)))]
       (if (and limit (not (s/valid? ::tx/limit limit)))
         (respond context (badrequest {:error "Limit must be between 1 and 1000"}))
         (assoc context :query-params query-params))))})

(def pagination
  {:name :pagination
   :enter
   (fn [context]
     (if-let [after (get-in context [:request :query-params :starting_after])]
       (assoc context :starting-after after)
       context))})

(def list-transactions
  {:name :list-transactions
   :enter
   (fn [context]
     (let [query (:account context)
           transactions (tx/list-transactions query)
           totals (analytics.transactions/calculate-totals query)]
       (if transactions
         (respond context (ok {:transactions (map #(st/select-spec ::tx/pub-transaction %) transactions)
                             :total_debit (:total_debit totals)
                             :total_credit (:total_credit totals)
                             :balance (:balance totals)}))
         (respond context (not-found {:error "Account not found."})))))})

(def list-balances
  {:name :list-balances
   :enter
   (fn [context]
     (if-let [balances (b/list-balances (:account context))]
       (respond context (ok (map #(st/select-spec ::b/pub-balance %) balances)));
       (respond context (not-found {:error "Account not found."}))))})

(def auth
  {:name :auth
   :enter
   (fn [context]
     (if-let [customer (sec/apikey-auth context)]
       (let [ctx (assoc context :customer customer)]
         (analytics/identify ctx)
         ctx)
       (respond context (forbidden (:creds msg)))))})

(def from-balance
  {:name :from-balance
   :enter
   (fn [context]
     (if-let [from (b/balance (b/ctx->id context :from))]
       (assoc-in context [:from :balance] from)
       context))})

(def date-range-params
  {:name :date-range-params
   :enter
   (fn [context]
     (let [params (get-in context [:request :query-params])
           start-date (get params :startDate)
           end-date (get params :endDate)]
       (assoc context :date-range {:start-date start-date
                                  :end-date end-date})))})

(def period-param
  {:name :period-param
   :enter
   (fn [context]
     (if-let [period (get-in context [:request :query-params :period])]
       (if (contains? #{"daily" "weekly" "monthly"} period)
         (assoc context :period period)
         (respond context (badrequest {:error "Invalid period parameter"})))
       (respond context (badrequest {:error "Missing period parameter"}))))})

(def get-transaction-stats
  {:name :get-transaction-stats
   :enter
   (fn [context]
     (if-let [transactions (tx/list-transactions-for-accounts
                           (:accounts context)
                           (:date-range context))]
       (let [stats (analytics.transactions/calculate-stats transactions)]
         (respond context (ok stats)))
       (respond context (ok {:totalTransactions 0
                           :transactionVolume 0
                           :averageTransactionSize 0}))))})

(def get-transaction-trends
  {:name :get-transaction-trends
   :enter
   (fn [context]
     (if-let [transactions (tx/list-transactions-for-accounts
                           (:accounts context)
                           (:date-range context))]
       (let [trend-data (analytics.transactions/group-by-period
                        transactions
                        (:period context))]
         (respond context (ok {:data trend-data})))
       (respond context (ok {:data []}))))})

(def transaction-id-param
  {:name :transaction-id-param
   :enter
   (fn [context]
     (if-let [tx-id (get-in context [:request :path-params :transaction-id])]
       (let [id (merge {:transaction-id tx-id}
                      (select-keys (:customer context) [:public-key]))]
         (if-let [transaction (tx/tx-get id)]
           (respond context (ok (st/select-spec ::tx/pub-transaction transaction)))
           (respond context (not-found {:error "Transaction not found"}))))
       (respond context (badrequest {:error "Missing transaction ID"}))))})

(def routes
  (route/expand-routes
   #{["/v1/transactions"            :post
      [http/json-body auth parse-tx spec-tx from-balance genesis-balance check-balance hash-txs chain-txs]
      :route-name :transactions-post]
     ["/v1/stats"      :get
      [http/json-body auth account-queryparam date-range-params get-transaction-stats]
      :route-name :transactions-stats]
     ["/v1/trends"     :get
      [http/json-body auth account-queryparam period-param date-range-params get-transaction-trends]
      :route-name :transactions-trends]
     ["/v1/transactions/:transaction-id"            :get
      [http/json-body auth transaction-id-param]
      :route-name :transactions-get]
     ["/v1/transactions"            :get
      [http/json-body auth account-queryparam transaction-query-params pagination list-transactions]
      :route-name :transactions-list]
     ["/v1/balances"            :get
      [http/json-body auth account-queryparam list-balances]
      :route-name :balances-get]}))
