package com.example.democoroutines

import ch.qos.logback.core.util.ExecutorServiceUtil.newExecutorService
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.reactive.awaitFirst
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
import org.springframework.web.reactive.function.server.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Signal
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import reactor.util.context.Context
import reactor.util.function.Tuple2
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
        GET("/sync/people/{id}", peopleHandler::findById)
    }

    @Bean
    fun routeA(peopleHandler: PeopleHandler) = router {
        GET("/async/people/{id}", peopleHandler::asyncFindById)
        GET("/teste", peopleHandler::teste)
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
                    val mapper = ObjectMapper()
                            .registerModule(KotlinModule())
                            .registerModules(JavaTimeModule())
                            .registerModules(ParameterNamesModule())
                    mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)
                    mapper.configure(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE, false)
                    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    mapper.configure(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY, false)
                    mapper.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, false)
                    mapper.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true)
                    mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE, true)
                    it.defaultCodecs().jackson2JsonDecoder(Jackson2JsonDecoder(mapper))
                    it.defaultCodecs().jackson2JsonEncoder(Jackson2JsonEncoder(mapper))
                }.build()
    }

}

@Component
class PeopleHandler(val swapiClient: SWAPIClient) {

    private val log: Logger = LoggerFactory.getLogger(javaClass)
    val inicio: String = "inicio"
    val final: String = "final"

    suspend fun findById(request: ServerRequest): ServerResponse {
        val id = request.pathVariable("id").toInt()
        val map = doFind()
        return ServerResponse.ok().bodyValueAndAwait(map)
    }

    fun asyncFindById(request: ServerRequest): Mono<ServerResponse> {
        val id = request.pathVariable("id").toInt()
        return ServerResponse.ok().body(asyncDoFind())
    }

    fun teste(request: ServerRequest): Mono<ServerResponse> {
        val inicio = "inicio"
        val i = System.currentTimeMillis()
        val x = asyncDoFind()
                .doOnEach {
                    if (it.isOnComplete) {
                        val fim = System.currentTimeMillis()
                        it.context.getOrEmpty<Long>(inicio)
                                .ifPresent { ini ->
                                    println("tempo: ${fim - ini}")
                                }
                    }
                }
                .subscriberContext { it.put(inicio, i) }
        return ServerResponse.ok().body(x)
    }

    suspend fun doFind(): List<People> {
        val startTime = System.currentTimeMillis()
        val completed = coroutineScope {
            val people = swapiClient.getPeopleById(1)
            people.films = people.films?.parallelStream()
                    ?.map { url ->
                        val id = "\\d".toRegex().find(url)?.value?.toInt() ?: throw Exception()
                        runBlocking {
                            swapiClient.getFilm(id).title
                        }
                    }?.collect(Collectors.toList())

            people
        }

        val endTime = System.currentTimeMillis()
        log.info("[getPeopleById] total time [  ${endTime - startTime}  ]")
        return listOf(completed)
    }

    fun asyncDoFind(): Flux<People> {
        val collectList = swapiClient.asyncGetPeopleById(1)
                .flatMap { people: People ->
                    val x = Flux.fromIterable<String>(people.films!!)
                            .flatMap { url ->
                                val id = "\\d".toRegex().find(url)?.value?.toInt() ?: throw Exception()
                                swapiClient.asyncGetFilm(id)
                            }.map { film ->
                                film.title
                            }.collectList()
                            .zipWith(Mono.just(people))
                            .map { t ->
                                t.t2.films = t.t1
                                t.t2
                            }
                    x
                }
        return collectList.map { listOf(it) }.flatMapMany { Flux.fromIterable(it) }
//        return collectList
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

    fun asyncGetPeopleById(id: Int): Mono<People> {
        val startTime = System.currentTimeMillis()
        val awaitBody = webClient.get()
                .uri("/people/$id/")
                .retrieve()
                .bodyToMono<People>()
                .log("[API] getPeopleById")
        val endTime = System.currentTimeMillis()
        log.info("[getPeopleById] total time [  ${endTime - startTime}  ]")
        return awaitBody
    }

    suspend fun getPeopleById(id: Int): People {
        val startTime = System.currentTimeMillis()
        val awaitBody = webClient.get()
                .uri("/people/$id/")
                .awaitExchange()
                .awaitBody<People>()
        val endTime = System.currentTimeMillis()
        log.info("[asyncGetPeopleById] total time [  ${endTime - startTime}  ]")
        return awaitBody
    }

    fun asyncGetFilm(id: Int): Mono<Films> {
        val awaitBody = webClient.get()
                .uri("/films/$id/")
                .retrieve()
                .bodyToMono<Films>()
                .log("[API] getFilm")
        return awaitBody
    }

    suspend fun getFilm(id: Int): Films {
        val startTime = System.currentTimeMillis()
        val awaitBody = webClient.get()
                .uri("/films/$id/")
                .awaitExchange()
                .awaitBody<Films>()
        val endTime = System.currentTimeMillis()
        log.info("[asyncGetFilm] total time [  ${endTime - startTime}  ]")
        return awaitBody
    }
}

data class People(val name: String,
                  var films: List<String>? = null)

data class Films(val title: String)