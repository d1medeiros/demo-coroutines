package com.example.democoroutines

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.codec.json.Jackson2JsonDecoder
import org.springframework.http.codec.json.Jackson2JsonEncoder
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.*
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.bodyValueAndAwait
import org.springframework.web.reactive.function.server.coRouter
import reactor.core.publisher.Mono
import reactor.core.publisher.Signal
import reactor.util.context.Context
import java.util.function.Consumer
import java.util.stream.Collectors
import javax.annotation.PostConstruct

@SpringBootApplication
class DemoCoroutinesApplication

fun main(args: Array<String>) {
    runApplication<DemoCoroutinesApplication>(*args)
}


fun <T> logOnSignal(log: Logger, logCategory: String): Consumer<Signal<T>> {
    return Consumer { signal ->
        when {
            signal.isOnNext -> {
                val label = signal.context.getOrDefault(logCategory, "")
                log.info("[] $label : ${signal.get()}")
            }
            signal.isOnComplete -> {
                val label = signal.context.getOrDefault(logCategory, "")
                val url = signal.context.getOrDefault("URL", "")
                log.info("[] $url $label : complete")
            }
            signal.isOnError -> {
                val label = signal.context.getOrDefault(logCategory, "")
                val url = signal.context.getOrDefault("URL", "")

                try {
                    log.error("[] $url $label : ${signal.get() ?: signal.throwable?.message}", signal.throwable)
                } catch (e: Exception) {
                    log.error("[] $url $label : erro interno")
                }
            }
        }
    }
}

@Configuration
class Configurations {
    private val log: Logger = LoggerFactory.getLogger(javaClass)
    private val logCategory = "[WebClientConfig]"

    @Bean
    fun route(peopleHandler: PeopleHandler) = coRouter {
        GET("/swapi/people/{id}", peopleHandler::findById)
    }

    @Bean
    fun logRequest(): ExchangeFilterFunction {
        return ExchangeFilterFunction.ofRequestProcessor { clientRequest ->
            return@ofRequestProcessor Mono.just(clientRequest)
                    .doOnEach(logOnSignal(log, logCategory))
                    .subscriberContext(Context.of(logCategory, logCategory))
        }
    }

    @Bean
    fun strategies(): ExchangeStrategies {
        return ExchangeStrategies.builder()
                .codecs {
                    val mapper = ObjectMapper().registerKotlinModule()
                            .registerModules(JavaTimeModule())
                            .registerModules(ParameterNamesModule())
                            .registerModules(Jdk8Module())
                    mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)
                    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE, true)
                    it.defaultCodecs().jackson2JsonDecoder(Jackson2JsonDecoder(mapper))
                    it.defaultCodecs().jackson2JsonEncoder(Jackson2JsonEncoder(mapper))
                }.build()
    }


}

@Component
class PeopleHandler(val swapiClient: SWAPIClient) {

    private val log: Logger = LoggerFactory.getLogger(javaClass)

    suspend fun findById(request: ServerRequest): ServerResponse {
        val id = request.pathVariable("id").toInt()
        val map = doFind(id)
        return ServerResponse.ok().bodyValueAndAwait(map)
    }

    fun doFind(id: Int): List<People> {
        return runBlocking {
            val startTime = System.currentTimeMillis()
            val result = flow {
                for (i in 1..6) {
                    val peopleById = swapiClient.getPeopleById(i)
                    emit(peopleById)
                }
            }.toList()
            val endTime = System.currentTimeMillis()
            log.info("[PeopleHandler] [doFind] tempo: ${endTime - startTime}")
            return@runBlocking result
        }
    }

}

@Component
class SWAPIClient(val strategies: ExchangeStrategies, val logRequest: ExchangeFilterFunction) {
    private val log: Logger = LoggerFactory.getLogger(javaClass)
    @Value("\${swapi.uri}")
    private lateinit var uri: String

    private lateinit var webClient: WebClient

    @PostConstruct
    private fun init() {
        webClient = WebClient.builder()
                .baseUrl(uri)
                .exchangeStrategies(strategies)
                .filter(logRequest)
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build()
    }

    suspend fun getPeopleById(id: Int): People {
        val startTime = System.currentTimeMillis()
        val awaitBody = webClient.get()
                .uri("/people/$id/")
                .awaitExchange()
                .awaitBody<People>()
        val endTime = System.currentTimeMillis()
        log.info("[getPeopleById] total time [  ${endTime - startTime}  ]")
        return awaitBody
    }
}


/// 1- 2786