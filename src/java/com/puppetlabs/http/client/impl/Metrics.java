package com.puppetlabs.http.client.impl;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.http.HttpRequest;
import org.apache.http.RequestLine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class Metrics {
    public static final String METRIC_NAMESPACE = "puppetlabs.http-client.experimental";
    public static final String URL_NAMESPACE = "with-url";
    public static final String ID_NAMESPACE = "with-metric-id";
    public static final String BYTES_READ_STRING = "bytes-read";
    public enum MetricType { BYTES_READ; }

    public static String metricTypeString(Metrics.MetricType metricType) {
        // currently this is the only metric type we have; in the future when there are multiple types this will do something more useful
        return BYTES_READ_STRING;
    }

    private static ArrayList<Timer.Context> startBytesReadMetricIdTimers(MetricRegistry registry,
                                                                         String[] metricId) {
        ArrayList<Timer.Context> timers = new ArrayList<>();
        for (int i = 0; i < metricId.length; i++) {
            ArrayList<String> currentId = new ArrayList<>();
            for (int j = 0; j <= i; j++) {
                currentId.add(metricId[j]);
            }
            currentId.add(0, ID_NAMESPACE);
            currentId.add(currentId.size(), BYTES_READ_STRING);
            String metric_name = MetricRegistry.name(METRIC_NAMESPACE, currentId.toArray(new String[currentId.size()]));
            timers.add(registry.timer(metric_name).time());
        }
        return timers;
    }

    private static ArrayList<Timer.Context> startBytesReadUrlTimers(MetricRegistry registry,
                                                                    HttpRequest request) {
        final RequestLine requestLine = request.getRequestLine();
        final String urlName = MetricRegistry.name(METRIC_NAMESPACE, URL_NAMESPACE, requestLine.getUri(), BYTES_READ_STRING);
        final String urlAndVerbName = MetricRegistry.name(METRIC_NAMESPACE, URL_NAMESPACE, requestLine.getUri(),
                requestLine.getMethod(), BYTES_READ_STRING);
        ArrayList<Timer.Context> timers = new ArrayList<>();
        timers.add(registry.timer(urlName).time());
        timers.add(registry.timer(urlAndVerbName).time());
        return timers;
    }

    public static ArrayList<Timer.Context> startBytesReadTimers(MetricRegistry registry, HttpRequest request, String[] metricId) {
        if (registry != null) {
            ArrayList<Timer.Context> urlTimers = startBytesReadUrlTimers(registry, request);
            ArrayList<Timer.Context> allTimers = new ArrayList<>(urlTimers);
            if (metricId != null) {
                ArrayList<Timer.Context> metricIdTimers = startBytesReadMetricIdTimers(registry, metricId);
                allTimers.addAll(metricIdTimers);
            }
            return allTimers;
        }
        else {
            return null;
        }
    }

    public static Map<String, Timer> getClientMetrics(MetricRegistry metricRegistry){
        if (metricRegistry != null) {
            return metricRegistry.getTimers(new ClientMetricFilter.ClientFilter());
        } else {
            return null;
        }
    }

    public static Map<String, Timer> getClientMetricsWithUrl(MetricRegistry metricRegistry, final String url, final MetricType metricType){
        if (metricRegistry != null) {
            return metricRegistry.getTimers(new ClientMetricFilter.UrlFilter(url, metricType));
        } else {
            return null;
        }
    }

    public static Map<String, Timer> getClientMetricsWithUrlAndVerb(MetricRegistry metricRegistry, final String url, final String verb, final MetricType metricType){
        if (metricRegistry != null) {
            return metricRegistry.getTimers(new ClientMetricFilter.UrlAndVerbFilter(url, verb, metricType));
        } else {
            return null;
        }
    }

    public static Map<String, Timer> getClientMetricsWithMetricId(MetricRegistry metricRegistry, final String[] metricId, final MetricType metricType){
        if (metricRegistry != null) {
            return metricRegistry.getTimers(new ClientMetricFilter.MetricIdFilter(metricId, metricType));
        } else {
            return null;
        }
    }

    public static Map<String, ClientMetricData> computeClientMetricsData(Map<String, Timer> timers){
        Map<String, ClientMetricData> metricsData = new HashMap<>();
        if (timers != null) {
            for (SortedMap.Entry<String, Timer> entry : timers.entrySet()) {
                Timer timer = entry.getValue();
                String metricId = entry.getKey();
                Double mean = timer.getSnapshot().getMean();
                Long meanMillis = TimeUnit.NANOSECONDS.toMillis(mean.longValue());
                Long count = timer.getCount();
                Long aggregate = count * meanMillis;

                ClientMetricData data = new ClientMetricData(metricId, count, meanMillis, aggregate);
                metricsData.put(metricId, data);
            }
        }
        return metricsData;
    }

    public static Map<String, ClientMetricData> getClientMetricsData(MetricRegistry metricRegistry){
        Map<String, Timer> timers = getClientMetrics(metricRegistry);
        return computeClientMetricsData(timers);
    }

    public static Map<String, ClientMetricData> getClientMetricsDataWithUrl(MetricRegistry metricRegistry, String url, MetricType metricType){
        Map<String, Timer> timers = getClientMetricsWithUrl(metricRegistry, url, metricType);
        return computeClientMetricsData(timers);
    }

    public static Map<String, ClientMetricData> getClientMetricsDataWithUrlAndVerb(MetricRegistry metricRegistry, String url, String verb, MetricType metricType){
        Map<String, Timer> timers = getClientMetricsWithUrlAndVerb(metricRegistry, url, verb, metricType);
        return computeClientMetricsData(timers);
    }

    public static Map<String, ClientMetricData> getClientMetricsDataWithMetricId(MetricRegistry metricRegistry, String[] metricId, MetricType metricType){
        Map<String, Timer> timers = getClientMetricsWithMetricId(metricRegistry, metricId, metricType);
        return computeClientMetricsData(timers);
    }
}
