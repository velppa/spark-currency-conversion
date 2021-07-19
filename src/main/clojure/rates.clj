(ns rates
  (:require [cheshire.core :as json]
            [org.httpkit.client :as http]
            [clojure.string :as str]))


(defn now []
  (int (/ (System/currentTimeMillis) 1000)))


(defn interval
  "Returns validity interval [from to] starting after provided
  timestamp."
  [timestamp]
  (let [length (* 4 7 24 60 60)         ; 4 weeks in seconds
        div (int (/ timestamp length))
        effective-from (* (inc div) length)
        effective-to (+ effective-from length)]
    [effective-from effective-to]))

;; (interval (now))
;; => [1628121600 1630540800]


(defn increment
  "Downloads live exchange rates from currencylayer.com and turns them
  into increment."
  [timestamp]
  (let [api-key (System/getenv "CURRENCYLAYER_API_KEY")
        response (:body @(http/request
                          {:method       :get
                           :url          "http://api.currencylayer.com/live"
                           :query-params {:access_key api-key}}))
        ;; response (slurp "rates.json")
        rates (json/decode response true)
        [effective-from effective-to] (interval timestamp)]
    (->> rates
         :quotes
         (map (fn [[pair rate]]
                [(subs (name pair) 3)
                 (double (/ 1 rate))
                 timestamp
                 effective-from
                 effective-to])))))

;; (take 5 (increment (now)))
;; => (
;; ["COP" 2.620342821500445E-4 1626672651 1628121600 1630540800]
;; ["CZK" 0.04616848653606895 1626672651 1628121600 1630540800]
;; ["BRL" 0.19558016214768922 1626672651 1628121600 1630540800]
;; ["ARS" 0.010397334339739845 1626672651 1628121600 1630540800]
;; ["VND" 4.345158321084993E-5 1626672651 1628121600 1630540800])


(defn old-data
  "Returns a sequence of old data from filename without those entries
   that start with timestamp."
  [filename timestamp]
  (let [[increment-from _] (interval timestamp)]
    (->>
     filename
     slurp
     str/split-lines
     (map (fn [line]
            (let [[curr rate ts from to] (str/split line #",")]
              [curr
               (Double/parseDouble rate)
               (Integer/parseInt ts)
               (Integer/parseInt from)
               (Integer/parseInt to)])))
     (filter (fn [[_ _ _ from _]] (not= from increment-from))))))

;; (take 5 (drop 1000 (old-data "rates.csv" (now))))
;; => (
;; ["TZS" 4.3119171942112666E-4 1544387864 1545868800 1548288000]
;; ["MGA" 2.5599894140136947E-4 1544387864 1545868800 1548288000]
;; ["BGN" 0.6041617075059842 1544387864 1545868800 1548288000]
;; ["AZN" 0.5884038545159701 1544387864 1545868800 1548288000]
;; ["CLP" 0.0013210905703589632 1544387864 1545868800 1548288000])


(defn -main [{:keys [timestamp filename]
              :or   {timestamp (now)}}]
  (->>
   (increment timestamp)
   (concat (old-data filename timestamp))
   (map #(str/join "," %))
   (str/join "\n")
   (spit filename)))


(comment

  ;; generate history of currency rates
  (let [now
        timestamps (map (fn [i]
                          {:filename "rates.csv"
                           :timestamp (- now (* i 4 7 24 60 60))}) (take 40 (range)))]
    (doall (map -main timestamps)))

  ;; first iteration with two columns
  (->> "rates.json"
       slurp
       (#(json/decode % true))
       :quotes
       (map (fn [[pair quote]]
              (format "%s,%s" (subs (name pair) 3) (double (/ 1 quote)))))
       (cons "currency,rate")
       (str/join "\n")
       (spit "rates.csv"))

  (* 30.5 24 60 60)

  (int (/ 1626436324 (* 4 7 24 60 60))))
