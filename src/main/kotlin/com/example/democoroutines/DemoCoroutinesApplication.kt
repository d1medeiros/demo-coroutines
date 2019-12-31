package com.example.democoroutines

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule
import kotlinx.coroutines.coroutineScope
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
import org.springframework.web.reactive.function.client.ExchangeStrategies
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import org.springframework.web.reactive.function.client.awaitExchange
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.bodyValueAndAwait
import org.springframework.web.reactive.function.server.coRouter
import javax.annotation.PostConstruct

@SpringBootApplication
class DemoCoroutinesApplication

fun main(args: Array<String>) {
    runApplication<DemoCoroutinesApplication>(*args)
}

@Configuration
class Configurations {
	private val log: Logger = LoggerFactory.getLogger(javaClass)

	@Bean
    fun route(peopleHandler: PeopleHandler) = coRouter {
		GET("/swapi/people/{id}", peopleHandler::findById)
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
		val peopleById = doFind(id)
		return ServerResponse.ok().bodyValueAndAwait(peopleById)
    }

	fun doFind(id: Int): People {
		return runBlocking {
			 swapiClient.getPeopleById(id)
		}
	}

}

@Component
class SWAPIClient(val strategies: ExchangeStrategies){
	private val log: Logger = LoggerFactory.getLogger(javaClass)
	@Value("\${swapi.uri}")
	private lateinit var uri: String

	private lateinit var webClient: WebClient

	@PostConstruct
	private fun init() {
		webClient = WebClient.builder()
				.baseUrl(uri)
				.exchangeStrategies(strategies)
				.defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
				.build()
	}

	suspend fun getPeopleById(id: Int):People {
		val startTime = System.currentTimeMillis()
		val awaitBody = webClient.get()
				.uri("/people/$id/")
				.awaitExchange()
				.awaitBody<People>()
		val endTime = System.currentTimeMillis()
		log.info("[getPeopleById] FOUND PEOPLE ID: $awaitBody total time [  ${endTime - startTime}  ]")
		return awaitBody
	}
}


/// 1- 2786