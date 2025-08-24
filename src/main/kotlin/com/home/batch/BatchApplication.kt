package com.home.batch

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.JobParametersIncrementer
import org.springframework.batch.core.Step
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.StepExecutionListener
import org.springframework.batch.core.annotation.AfterStep
import org.springframework.batch.core.annotation.BeforeStep
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.CompositeJobParametersValidator
import org.springframework.batch.core.job.DefaultJobParametersValidator
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.core.listener.ExecutionContextPromotionListener
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.support.ListItemReader
import org.springframework.batch.repeat.CompletionPolicy
import org.springframework.batch.repeat.RepeatContext
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.transaction.PlatformTransactionManager
import java.time.LocalDateTime
import java.util.UUID
import kotlin.random.Random

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
            )
            .listener(promotionListener())
            .build()
    }

    @Bean
    fun step2(): Step {
        return StepBuilder("step2", jobRepository)
            .tasklet(
                taskLet2(),
                transactionManager
            ).build()
    }

    @Bean
    fun job1(): Job {
        return JobBuilder("job1", jobRepository)
            .start(step())
            .next(step2())
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
        val test = chunkContext.stepContext
            .jobParameters["test"]
            .toString()

        val executionContext = chunkContext.stepContext
            .stepExecution
            //            .jobExecution
            .executionContext
        executionContext.put("user.name", test)

        logger.error("hello, world: name = $executionContext")
        logger.error("hello, world: name = $name")
        logger.error("hello, world: name = $test")
        RepeatStatus.FINISHED
    }


    @Bean
    fun taskLet2(
    ) = Tasklet {
            contribution,
            chunkContext,
        ->

        val executionContext = chunkContext.stepContext
            .stepExecution
            .jobExecution
            .executionContext

        val test = executionContext.get("user.name")
            .toString()

        logger.error("hello, world2: name = $executionContext")
        logger.error("hello, world2: name = $test")
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

    @Bean
    fun promotionListener(): StepExecutionListener {
        val listener = ExecutionContextPromotionListener()
        listener.setKeys(arrayOf("user.name"))

        return listener
    }

    @Bean
    fun chunkJob(): Job {
        return JobBuilder("chunkJob", jobRepository)
            .start(chunkStep())
            .build()
    }

    @Bean
    fun chunkStep(): Step {
        val policy = object : CompletionPolicy {
            private var totalProcessed: Int = 0
            private var chunk: Int = 0
            override fun isComplete(executionContext: RepeatContext, result: RepeatStatus): Boolean {
                if (RepeatStatus.FINISHED == result) {
                    return true
                }
                return isComplete(executionContext)
            }

            override fun isComplete(executionContext: RepeatContext): Boolean {
                return totalProcessed >= chunk
            }

            override fun start(parent: RepeatContext): RepeatContext {
                this.chunk = Random.nextInt(20)
                this.totalProcessed = 0
                println("chunk is : $chunk")
                return parent
            }

            override fun update(executionContext: RepeatContext) {
                this.totalProcessed += 1
            }
        }

        return StepBuilder("step3", jobRepository)
            //            .chunk<String, String>(1000, transactionManager)
            .chunk<String, String>(policy, transactionManager)
            .reader(chunkReader())
            .writer(chunkWriter())
            .listener(LoggingStepStartStopListener())
            .build()
    }

    @Bean
    @StepScope
    fun chunkReader(): ListItemReader<String> {
        val uuids = List(100) { UUID.randomUUID().toString() }
        return ListItemReader<String>(uuids)
    }

    @Bean
    fun chunkWriter(): ItemWriter<String> {
        return ItemWriter { items ->
            println("==================")
            items.forEachIndexed { index, item ->
                println(">> current item = $item, index = $index")
            }
        }
    }

    //    @Bean
    //    fun chunkStep1(): Step {
    //        return StepBuilder("chunkStep1", jobRepository)
    //            .chunk<String, String>(10, transactionManager)
    //            .reader()
    //            .build()
    //    }
    //
    //    @Bean
    //    @StepScope
    //    fun reader(
    //        @Value("#{jobParameters['name']}") name: String?,
    //    ) : FlatFileItemReader<String> {
    //
    //    }

    @Bean
    fun passSuccessFailJob(): Job {
        return JobBuilder("passSuccessFailJob", jobRepository)
            .start(passStep())
            .on("FAILED").to(failStep())
            .from(passStep()).on("*").to(successStep())
            .end()
            .build()
    }

    @Bean
    fun passTask(): Tasklet {
        return Tasklet { contribution, chunkContext ->
            println("PASS")
            throw RuntimeException()
//            RepeatStatus.FINISHED
        }
    }

    @Bean
    fun successTask(): Tasklet {
        return Tasklet { contribution, chunkContext ->
            println("SUCCESS")
            RepeatStatus.FINISHED
        }
    }

    @Bean
    fun failTask(): Tasklet {
        return Tasklet { contribution, chunkContext ->
            println("FAIL")
            RepeatStatus.FINISHED
        }
    }

    @Bean
    fun passStep(): Step {
        return StepBuilder("passStep", jobRepository)
            .tasklet(
                passTask(),
                transactionManager
            )
            .build()
    }

    @Bean
    fun successStep(): Step {
        return StepBuilder("successStep", jobRepository)
            .tasklet(
                successTask(),
                transactionManager
            )
            .build()
    }

    @Bean
    fun failStep(): Step {
        return StepBuilder("failStep", jobRepository)
            .tasklet(
                failTask(),
                transactionManager
            )
            .build()
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

    class LoggingStepStartStopListener {
        @BeforeStep
        fun beforeStep(step: StepExecution) {
            println(step.stepName + "has been started")
        }

        @AfterStep
        fun afterStep(step: StepExecution): ExitStatus {
            println(step.stepName + "has ended, $step.exitStatus")

            return step.exitStatus
        }
    }
}

fun main(args: Array<String>) {
    runApplication<BatchApplication>(*args)
}
