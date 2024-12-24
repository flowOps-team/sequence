(ns decimals.analytics.transactions
  (:require [decimals.dynamodb :as db]
            [clojure.tools.logging :as log]
            [java-time :as t]))

(defn calculate-stats
  "Calculates statistical metrics for a collection of transactions.
   
   Args:
     transactions - Collection of transaction maps, each containing :amount key
   
   Returns a map containing:
     :totalTransactions - Total number of transactions
     :transactionVolume - Sum of all transaction amounts
     :averageTransactionSize - Average amount per transaction"
  [transactions]
  (let [total-count (count transactions)
        total-volume (reduce + (map :amount transactions))
        avg-size (if (pos? total-count)
                  (/ total-volume total-count)
                  0)]
    {:totalTransactions total-count
     :transactionVolume total-volume
     :averageTransactionSize avg-size}))

(defn calculate-trends [current-stats previous-stats]
  {:totalTransactions (:totalTransactions current-stats)
   :percentageChange (if (pos? (:totalTransactions previous-stats))
                      (* 100 (/ (- (:totalTransactions current-stats)
                                  (:totalTransactions previous-stats))
                               (:totalTransactions previous-stats)))
                      0)
   :volumeChange (- (:transactionVolume current-stats)
                   (:transactionVolume previous-stats))})

(defn group-by-period [transactions period]
  (let [group-fn (case period
                  "daily" #(-> (t/instant (:date %))
                              (t/local-date-time (t/zone-id "UTC"))
                              t/local-date
                              str)
                  "weekly" #(-> (t/instant (:date %))
                               (t/local-date-time (t/zone-id "UTC"))
                               t/local-date
                               (.with (t/day-of-week 1))
                               str))]
    (->> transactions
         (group-by group-fn)
         (map (fn [[date txs]]
                {:date date
                 :count (count txs)
                 :volume (reduce + (map :amount txs))})))))

(defn calculate-totals
  "Calculates total debits and credits for an account using DynamoDB aggregation.
   
   Args:
     query - Query parameters including account information
   
   Returns:
     Map containing :total_debit, :total_credit, and :balance"
  [query]
  (let [totals (db/aggregate-account-totals query)
        balance (- (:credit totals) (:debit totals))]
    {:total_debit (:debit totals)
     :total_credit (:credit totals)
     :balance balance}))
