package com.home.batch

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.JobParametersIncrementer
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.CompositeJobParametersValidator
import org.springframework.batch.core.job.DefaultJobParametersValidator
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.transaction.PlatformTransactionManager
import java.time.LocalDateTime

@SpringBootApplication
class BatchApplication(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
) {
    private val logger: Logger = LoggerFactory.getLogger(BatchApplication::class.java)

    @Bean
    fun step(): Step {
        return StepBuilder("step1", jobRepository)
            .tasklet(
                taskLet(null),
                transactionManager
            ).build()
    }

    @Bean
    fun job1(): Job {
        return JobBuilder("job1", jobRepository)
            .start(step())
            .validator(validator())
            .incrementer(DailyJobTimeStamper())
            .build()
    }

    @StepScope
    @Bean
    fun taskLet(
        @Value("#{jobParameters['name']}") name: String? = null,
    ) = Tasklet {
            contribution,
            chunkContext,
        ->
        logger.error("hello, world: name = $name")
        RepeatStatus.FINISHED
    }

    @Bean
    fun validator(): CompositeJobParametersValidator {
        val validator = CompositeJobParametersValidator()

        val default = DefaultJobParametersValidator(
            arrayOf("name"),
            arrayOf("")
        )
        default.afterPropertiesSet()

        validator.setValidators(
            listOf(default)
        )
        return validator
    }

    class DailyJobTimeStamper : JobParametersIncrementer {
        override fun getNext(parameters: JobParameters?): JobParameters {
            val builder = parameters?.let { JobParametersBuilder(it) }
                ?: JobParametersBuilder()

            return builder
                .addLocalDateTime("currentDate", LocalDateTime.now())
                .toJobParameters()
        }
    }
}

fun main(args: Array<String>) {
    runApplication<BatchApplication>(*args)
}
