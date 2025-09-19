package com.example.batchexample.databaseBatch;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.Map;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class OrderRecoveryJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;

    @Bean
    public Job orderRecoveryJob(){
        return new JobBuilder("orderRecoveryJob", jobRepository)
                .start(orderRecoveryStep())
                .build();
    }

    @Bean
    public Step orderRecoveryStep() {
        return new StepBuilder("orderRecoveryStep", jobRepository)
                .<HackedOrder, HackedOrder>chunk(5, transactionManager)
                .reader(compromisedOrderReader())
                .processor(orderStatusProcessor())
                .writer(orderStatusWriter())
                .build();
    }

    @Bean
    public JdbcPagingItemReader compromisedOrderReader() {
        return new JdbcPagingItemReaderBuilder<HackedOrder>()
                .name("compromisedOrderReader")
                .dataSource(dataSource)
                .pageSize(10)
                .selectClause("SELECT *")
                .fromClause("FROM orders")
                .whereClause("WHERE (status = 'SHIPPED' AND shipping_id is null)" +
                        "OR (status = 'CANCELLED' AND shipping_id is not null)")
                .sortKeys(Map.of("id", Order.ASCENDING))
                .beanRowMapper(HackedOrder.class)
                .build();
    }

    @Bean
    public ItemProcessor<HackedOrder, HackedOrder> orderStatusProcessor() {
        return order -> {
            if(order.getShippingId() == null){
                order.setStatus("READY_FOR_SHIPMENT");
            }else {
                order.setStatus("SHIPPED");
            }
            return order;
        };
    }

    @Bean
    public JdbcBatchItemWriter<HackedOrder> orderStatusWriter() {
        return new JdbcBatchItemWriterBuilder<HackedOrder>()
                .dataSource(dataSource)
                .sql("UPDATE orders SET status = :status WHERE id = :id")
                .beanMapped()
                .assertUpdates(true)
                .build();
    }

    @Getter
    @Setter
    @NoArgsConstructor
    public static class HackedOrder{
        private Long id;
        private Long customerId;
        private LocalDateTime orderDateTime;
        private String status;
        private String shippingId;

        public String getShippingId() {
            return shippingId;
        }
        public void setStatus(String status) {
            this.status = status;
        }
    }
}
