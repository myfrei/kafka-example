package com.kafka.demo.case3.reporting.service;

import com.kafka.demo.case3.reporting.model.BatchSummary;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Агрегирует сводки батчей в общий отчёт.
 *
 * reporting-service — downstream-потребитель: батч-консьюмер после коммита
 * каждого батча публикует {@link BatchSummary}, а этот сервис строит из них
 * накопительную статистику.
 */
@Slf4j
@Service
public class ReportingService {

    private final AtomicLong batchCount = new AtomicLong();
    private final AtomicLong recordCount = new AtomicLong();
    private final AtomicReference<BigDecimal> totalRevenue = new AtomicReference<>(BigDecimal.ZERO);

    public void accept(BatchSummary summary) {
        batchCount.incrementAndGet();
        recordCount.addAndGet(summary.recordCount());
        totalRevenue.updateAndGet(current -> current.add(summary.totalRevenue()));
        log.info("Report updated by batch {}: batches={} records={} revenue={}",
            summary.batchId(), batchCount.get(), recordCount.get(), totalRevenue.get());
    }

    public long batchCount() {
        return batchCount.get();
    }

    public long recordCount() {
        return recordCount.get();
    }

    public BigDecimal totalRevenue() {
        return totalRevenue.get();
    }
}
