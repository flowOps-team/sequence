(ns decimals.transactions
  "Namespace for handling financial transactions between accounts.
   Provides functionality for creating, retrieving, and processing
   transactions using DynamoDB as the storage backend."
  (:require [decimals.dynamodb :as db]
            [clojure.tools.logging :as log]
            [decimals.crypto :as crypto]
            [decimals.balances :as b]
            [spec-tools.core :as st]
            [clojure.data.json :as json]
            [clojure.spec.alpha :as s]
            [java-time :as t]))

(defn account->id
  "Generates a unique identifier for an account based on public key and party.
   Args:
     account - Map containing account details
     party - Keyword indicating the party type (:from or :to)"
  [account party]
  (str (:public-key account) "#" ((keyword party) account)))

(defn tx-get [id]
  (db/query id))

(defn item->tuple [item]
  (log/debug "putting " item)
  [:put item])

(defn doc->put [doc]
  (assoc {:table-name :decimals}
          :item doc
          :cond-expr "attribute_not_exists(#SK)"
          :expr-attr-names {"#SK" (:id doc)}))

(defn tx->doc [tx]
  (conj tx {:PK (account->id tx (:party tx))
            :SK (:id tx)
            :GSI1_PK (:public-key tx)
            :GSI1_SK ((keyword (:party tx)) tx)
            :LSI1_SK (:timestamp tx)}))

(defn chain
  "Processes a chain of transactions by creating balance and transaction records.
   Takes a context map containing :from and :to transaction details and balances.
   Returns updated context with :success or :error status."
  [context]
  (let [from-balance (get-in context [:from :balance])
        from-tx (get-in context [:from :tx])
        to-balance (get-in context [:to :balance])
        to-tx (get-in context [:to :tx])
        txs (list from-balance from-tx to-balance to-tx)]

    (let [items (->> txs
                     ;(filter map?) ;FIXME: fail in invalid data is present
                     (map tx->doc)
                     (map doc->put)
                     (map item->tuple))]

      (if-let [created (db/transact-put items)]
        (assoc context :success [from-tx to-tx])
        (assoc context :error :internal)))))

(defn context->account
  "Extracts account information from the transaction context for a given party.
   Args:
     context - Transaction context map
     party - Keyword indicating which party's account to extract (:from or :to)
   Returns account map if valid according to ::account spec, logs warning if invalid."
  [context party]
  (let [tx (:tx context)
        cust (:customer context)
        party (select-keys tx [party])
        pub-key (select-keys cust [:public-key])
        currency (select-keys tx [:currency]) 
        account (conj party pub-key currency)]
    (if (s/valid? ::account account)
      account
      (log/warn (s/explain ::account account)))))

(defn party-funds
  "Retrieves the current balance for a party in the transaction.
   Args:
     context - Transaction context map
     party - Keyword indicating which party's funds to check
   Returns balance information if found and valid, nil otherwise."
  [context party]
  (when-let [query (b/ctx->id context party)]
    (when-let [balance (b/balance query)]
      (if (s/valid? ::b/balance-tx balance)
        (let []
          (log/debug "got balance: " balance)
          balance)
        (log/warn "invalid balance in database: " balance (s/explain-data ::b/balance-tx balance))))))

(defn hash-txs
  "Creates hashed transaction records for both parties in a transaction.
   Takes a context map and generates transaction documents with:
   - Unique IDs based on MD5 hashes
   - Updated balances
   - Timestamps
   - Transaction types (debit/credit)
   Returns updated context with transaction details or nil if invalid."
  [context]
  (let [to-account (context->account context :to)
        from-account (context->account context :from)]
       (when-let [to (cond ;FIXME: call the each function only once (as-> ?)
                     (party-funds context :to) (party-funds context :to)
                     (b/account->genesis to-account :to) (b/account->genesis to-account :to))]
         (let [tx (:tx context)
               from (get-in context [:from :balance])
               from-tx (conj tx
                             {:id (crypto/map->md5 (st/select-spec ::pub-transaction from))
                              :public-key (:public-key from-account)
                              :party :from
                              :account (:from from-account)
                              :balance (- (:balance from) (:amount tx))
                              :date (str (t/instant))
                              :timestamp (str (t/to-millis-from-epoch (t/instant)))
                              :type :debit})
               to-tx (conj tx
                           {:id (crypto/map->md5 (st/select-spec ::pub-transaction to))
                            :public-key (:public-key to-account)
                            :party :to
                            :account (:to to-account)
                            :balance (+ (:balance to) (:amount tx))
                            :date (str (t/instant))
                            :timestamp (str (t/to-millis-from-epoch (t/instant)))
                            :type :credit})]
           (-> context
               (assoc-in [:to :balance] to)
               (assoc-in [:to :tx] to-tx)
               (assoc-in [:from :tx] from-tx))))))

(defn list-transactions
  "Retrieves a list of transactions matching the provided query.
   Args:
     query - Map containing query parameters for filtering transactions
   Returns sequence of transaction records or nil if none found."
  [query]
  (when-let [transactions (db/list-transactions query)]
    (let []
      (log/debug transactions))
      transactions
    ))

(defn list-transactions-for-accounts
  "Retrieves transactions for multiple accounts and merges results.
   Args:
     accounts - Sequence of account maps
     query - Additional query parameters
   Returns merged sequence of transaction records"
  [accounts query]
  (->> accounts
       (pmap #(list-transactions (merge query {:public-key (:public-key %)
                                             :account (:account %)})))
       (filter identity)
       (apply concat)
       (sort-by :date #(compare %2 %1))
       (take 10000)))
