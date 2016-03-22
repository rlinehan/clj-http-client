(ns com.puppetlabs.http.client.impl.metrics-unit-test
  (:require [clojure.test :refer :all])
  (:import (com.codahale.metrics MetricRegistry)
           (com.puppetlabs.http.client.impl Metrics Metrics$MetricType)
           (org.apache.http.message BasicHttpRequest)))

(def bytes-read Metrics$MetricType/BYTES_READ)

(defn add-metric-ns [string]
  (str "puppetlabs.http-client.experimental." string))

(deftest start-bytes-read-timers-test
  (testing "startBytesReadTimers creates the right timers"
    (let [url-id "puppetlabs.http-client.experimental.with-url.http://localhost/foo.bytes-read"
          url-verb-id "puppetlabs.http-client.experimental.with-url.http://localhost/foo.GET.bytes-read"]
      (testing "metric id timers are not created for a request without a metric id"
        (let [metric-registry (MetricRegistry.)]
          (Metrics/startBytesReadTimers metric-registry
                                        (BasicHttpRequest. "GET" "http://localhost/foo")
                                        nil)
          (= (set (list url-id url-verb-id (set (keys (.getTimers metric-registry))))))))
      (testing "metric id timers are not created for a request with an empty metric id"
        (let [metric-registry (MetricRegistry.)]
          (Metrics/startBytesReadTimers metric-registry
                                        (BasicHttpRequest. "GET" "http://localhost/foo")
                                        (into-array String []))
          (= (set (list url-id url-verb-id (set (keys (.getTimers metric-registry))))))))
      (testing "metric id timers are created correctly for a request with a metric id"
        (let [metric-registry (MetricRegistry.)]
          (Metrics/startBytesReadTimers metric-registry
                                        (BasicHttpRequest. "GET" "http://localhost/foo")
                                        (into-array ["foo" "bar" "baz"]))
          (= (set (list url-id url-verb-id
                        "puppetlabs.http-client.experimental.with-metric-id.foo"
                        "puppetlabs.http-client.experimental.with-metric-id.foo.bar"
                        "puppetlabs.http-client.experimental.with-metric-id.foo.bar.baz"))
             (set (keys (.getTimers metric-registry)))))))))

(defn create-and-stop-timers! [registry req id]
  (doseq [timer (Metrics/startBytesReadTimers
                 registry
                 req
                 id)]
    (.stop timer)))

(deftest get-client-metrics-data-test
  (let [metric-registry (MetricRegistry.)
        url "http://test.com/one"
        url2 "http://test.com/one/two"]
    (create-and-stop-timers! metric-registry (BasicHttpRequest. "GET" url) nil)
    (create-and-stop-timers! metric-registry (BasicHttpRequest. "POST" url) nil)
    (create-and-stop-timers! metric-registry (BasicHttpRequest. "POST" url)
                             (into-array ["foo" "bar"]))
    (create-and-stop-timers! metric-registry (BasicHttpRequest. "GET" url2)
                             (into-array ["foo" "abc"]))
    (testing "getClientMetrics without args returns all timers"
      (is (= (set
              ["puppetlabs.http-client.experimental.with-url.http://test.com/one.bytes-read"
               "puppetlabs.http-client.experimental.with-url.http://test.com/one.GET.bytes-read"
               "puppetlabs.http-client.experimental.with-url.http://test.com/one.POST.bytes-read"
               "puppetlabs.http-client.experimental.with-metric-id.foo.bytes-read"
               "puppetlabs.http-client.experimental.with-metric-id.foo.bar.bytes-read"
               "puppetlabs.http-client.experimental.with-url.http://test.com/one/two.bytes-read"
               "puppetlabs.http-client.experimental.with-url.http://test.com/one/two.GET.bytes-read"
               "puppetlabs.http-client.experimental.with-metric-id.foo.abc.bytes-read"])
             (set (keys (Metrics/getClientMetrics metric-registry)))
             (set (keys (Metrics/getClientMetricsData metric-registry))))))
    (testing "getClientMetricsData with url returns the right thing"
      (let [data (Metrics/getClientMetricsDataWithUrl metric-registry url bytes-read)]
        (is (= (add-metric-ns "with-url.http://test.com/one.bytes-read") (first (keys data))))
        (is (= 3 (.getCount (first (vals data))))))
      (let [data (Metrics/getClientMetricsDataWithUrl metric-registry url2 bytes-read)]
        (is (= (add-metric-ns "with-url.http://test.com/one/two.bytes-read") (first (keys data))))
        (is (= 1 (.getCount (first (vals data)))))))
    (testing "getClientMetricsData with url and method returns the right thing"
      (let [data (Metrics/getClientMetricsDataWithUrlAndVerb metric-registry url "GET" bytes-read)]
        (is (= (add-metric-ns "with-url.http://test.com/one.GET.bytes-read") (first (keys data))))
        (is (= 1 (.getCount (first (vals data))))))
      (let [data (Metrics/getClientMetricsDataWithUrlAndVerb metric-registry url "POST" bytes-read)]
        (is (= (add-metric-ns "with-url.http://test.com/one.POST.bytes-read") (first (keys data))))
        (is (= 2 (.getCount (first (vals data))))))
      (let [data (Metrics/getClientMetricsDataWithUrlAndVerb metric-registry url2 "GET" bytes-read)]
        (is (= (add-metric-ns "with-url.http://test.com/one/two.GET.bytes-read") (first (keys data))))
        (is (= 1 (.getCount (first (vals data)))))))
    (testing "getClientMetricsData with metric id returns the right thing"
      (let [data (Metrics/getClientMetricsDataWithMetricId
                  metric-registry (into-array ["foo"]) bytes-read)]
        (is (= (add-metric-ns "with-metric-id.foo.bytes-read") (first (keys data))))
        (is (= 2 (.getCount (first (vals data))))))
      (let [data (Metrics/getClientMetricsDataWithMetricId
                  metric-registry (into-array ["foo" "bar"]) bytes-read)]
        (is (= (add-metric-ns "with-metric-id.foo.bar.bytes-read") (first (keys data))))
        (is (= 1 (.getCount (first (vals data))))))
      (let [data (Metrics/getClientMetricsDataWithMetricId
                  metric-registry (into-array ["foo" "abc"]) bytes-read)]
        (is (= (add-metric-ns "with-metric-id.foo.abc.bytes-read") (first (keys data))))
        (is (= 1 (.getCount (first (vals data)))))))))
