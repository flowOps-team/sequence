(ns decimals.dynamodb
  (:import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder)
  (:require [taoensso.faraday :as far]
            [environ.core :refer [env]]
            [clojure.tools.logging :as log]))

;FIXME: persist connection
(def client-opts {
                  :access-key (:aws-access-key-id env)
                  :secret-key (:aws-secret-access-key env)
                  :endpoint (:dynamodb-endpoint env)
                  })

(defn query [k]
  (far/query client-opts :decimals
             {:PK [:eq k]}))

(defn query->pk [query]
  (str (:public-key query) "#" (:account query)))

(defn query-balance
  [acc-id currency]
  (let [balance (first (far/query client-opts :decimals
                                  {:PK [:eq acc-id]}
                                  {:query-filter {:currency [:eq currency]}
                                   :index :LSI1 ;FIXME: use LSI instead of GSI as partition is the same
                                   :order :desc
                                        ;:limit 1 ;FIXME: currencies are not optmized at the top of the partition
                                   }))]
    balance))

(defn pagination [query config]
  (if-let [after (:starting-after query)]
    (if-let [sk (:timestamp (far/get-item client-opts
                              :decimals
                              {:PK (query->pk query) :SK (:starting-after query)}))]
      (let []
        (log/debug "Paginated: " query config)
        (assoc config :last-prim-kvs {:PK (query->pk query)
                                      :SK (:starting-after query)
                                      :GSI1_PK (query->pk query)
                                      :GSI1_SK sk}))
      config)
    config))

(defn list-transactions [query]
  (let [base-config {:index :LSI1
                     :order :desc
                     :limit (or (:limit query) 1000)}
        key-conds (cond-> {:PK [:eq (query->pk query)]}
                   (and (:start-date query) (:end-date query))
                   (assoc :LSI1_SK [:between (:start-date query) (:end-date query)])
                   
                   (:start-date query)
                   (assoc :LSI1_SK [:ge (:start-date query)])
                   
                   (:end-date query)
                   (assoc :LSI1_SK [:le (:end-date query)]))]
    (->> base-config
         (pagination query)
         (far/query client-opts :decimals key-conds))))

(defn list-with-genesis [query]
  (let [pk (query->pk query)]
    (log/debug "Querying " pk)
    (when-let [gens (far/query client-opts :decimals
                               {:PK [:eq pk]
                                :SK [:begins-with (:account query)]}
                               {:order :desc
                                })]
      (let []
        (log/debug "Got genesis " gens)
        (->> gens
             (map :currency)
             (map #(query-balance pk %))))
      )))

(defn list-accounts [pk]
  (log/debug "Querying " pk)
  (far/query client-opts :decimals
             {:PK [:eq pk]}
             {:order :desc}))

(defn list-balances [query]
  (log/debug "Querying " query)
  (if (contains? query :account)
    (list-with-genesis query)
    (list-accounts (:public-key query))))

(defn put [item]
  (far/put-item client-opts :decimals item))

(defn transact-put [items]
  (far/transact-write-items client-opts {:items items}))

(defn aggregate-account-totals
  "Performs aggregation query for total debits and credits for an account"
  [query]
  (let [pk (query->pk query)]
    (try
      (let [transactions (far/query client-opts :decimals
                                  {:PK [:eq pk]}
                                  {:index :LSI1
                                   :consistent true
                                   :select ["amount" "type"]})]
        {:debit (->> transactions
                    (filter #(= (:type %) "debit"))
                    (map :amount)
                    (reduce + 0))
         :credit (->> transactions
                    (filter #(= (:type %) "credit"))
                    (map :amount)
                    (reduce + 0))})
      (catch Exception e
        (log/error "Error calculating totals:" e)
        {:debit 0 :credit 0}))))

(defn aggregate-transactions
  "Performs server-side aggregation of transactions using DynamoDB"
  [accounts period]
  (try
    (let [results (for [account accounts
                       :let [pk (query->pk account)]
                       :when pk]
                   (far/query client-opts 
                            :decimals
                            {:PK [:eq pk]}
                            {:index :LSI1
                             :select ["amount" "type" "date"]
                             :consistent true}))
          all-transactions (apply concat results)]
      {:total (count all-transactions)
       :volume (reduce + (map :amount all-transactions))
       :transactions all-transactions})
    (catch Exception e
      (log/error "Error aggregating transactions:" e)
      nil)))
