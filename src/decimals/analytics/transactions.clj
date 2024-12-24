(ns decimals.analytics.transactions
  (:require [decimals.dynamodb :as db]
            [clojure.tools.logging :as log]
            [java-time :as t])
  (:import [java.time.temporal WeekFields]
           [java.time LocalDate ZoneId]))

(defn calculate-stats
  "Calculates all-time statistical metrics for transactions.
   Returns a map with total transactions, volume and average size."
  [transactions]
  (let [total-count (count transactions)
        total-volume (reduce + (map :amount transactions))
        avg-size (if (pos? total-count)
                  (/ total-volume total-count)
                  0)]
    {:totalTransactions total-count
     :transactionVolume total-volume 
     :averageTransactionSize avg-size}))

(defn- get-week-number [date]
  (let [local-date (-> date
                      t/instant
                      (t/local-date-time (t/zone-id "UTC"))
                      t/local-date)
        week-fields (WeekFields/ISO)
        week-number (.get local-date (.weekOfWeekBasedYear week-fields))]
    week-number))

(defn- get-month-number [date]
  (-> date
      t/instant
      (t/local-date-time (t/zone-id "UTC"))
      t/local-date
      (.getMonthValue)))

(defn- get-year [date]
  (-> date
      t/instant
      (t/local-date-time (t/zone-id "UTC"))
      t/local-date
      (.getYear)))

(defn group-by-period
  "Groups transaction volumes by weekly or monthly periods.
   period can be 'weekly' or 'monthly'"
  [transactions period]
  (if (empty? transactions)
    []
    (let [group-fn (case period
                    "weekly" #(get-week-number (:date %))
                    "monthly" #(get-month-number (:date %)))
          grouped (->> transactions
                      (group-by group-fn))]
      (->> (case period
             "weekly" (range 1 54)
             "monthly" (range 1 13))
           (map (fn [period-num]
                 (let [txs (get grouped period-num [])]
                   {:period period-num
                    :volume (->> txs
                               (map :amount)
                               (reduce + 0))})))))))

(defn group-by-period-flow
  "Groups transaction flow by weekly or monthly periods.
   period can be 'weekly' or 'monthly'"
  [transactions period]
  (if (empty? transactions)
    []
    (let [group-fn (case period
                    "weekly" #(get-week-number (:date %))
                    "monthly" #(get-month-number (:date %)))
          grouped (->> transactions
                      (group-by group-fn))]
      (->> (case period
             "weekly" (range 1 54)
             "monthly" (range 1 13))
           (map (fn [period-num]
                 (let [txs (get grouped period-num [])]
                   {:period period-num
                    :inflow (->> txs
                               (filter #(= (:type %) "credit"))
                               (map :amount)
                               (reduce + 0))
                    :outflow (->> txs
                               (filter #(= (:type %) "debit"))
                               (map :amount)
                               (reduce + 0))})))))))
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
