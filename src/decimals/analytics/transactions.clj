(ns decimals.analytics.transactions
  (:require [decimals.dynamodb :as db]
            [clojure.tools.logging :as log]
            [java-time :as t]))

(defn calculate-stats [transactions]
  (let [total-count (count transactions)
        total-volume (reduce + (map :amount transactions))
        avg-size (if (pos? total-count)
                  (/ total-volume total-count)
                  0)
        success-count (count (filter #(= (:status %) "success") transactions))
        success-rate (if (pos? total-count)
                      (* 100 (/ success-count total-count))
                      0)]
    {:totalTransactions total-count
     :transactionVolume total-volume
     :averageTransactionSize avg-size
     :successRate success-rate}))

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
                  "daily" #(t/as-date (t/local-date (:date %)))
                  "weekly" #(t/as-date (t/beginning-of-week (t/local-date (:date %))))
                  "monthly" #(t/as-date (t/beginning-of-month (t/local-date (:date %)))))]
    (->> transactions
         (group-by group-fn)
         (map (fn [[date txs]]
                {:date (str date)
                 :count (count txs)
                 :volume (reduce + (map :amount txs))})))))
