import logging

import pika

from common.middleware.batch_controller import (
	_DestinationBatchPublisher,
	_deliver_to_user_callback_transparently,
)
from common.middleware.rabbitmq_config import (
	RABBITMQ_SETTINGS,
	rabbitmq_connection_parameters,
)
from .middleware import (
	MessageMiddlewareCloseError,
	MessageMiddlewareDisconnectedError,
	MessageMiddlewareMessageError,
	MessageMiddlewareQueue,
	MessageMiddlewareExchange,
	MessageMiddlewareExchangePublisher,
)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

	# Inicializa la conexion con RabbitMQ y el canal de comunicacion.
	# La cola de consumo se declara lazy al iniciar start_consuming.
	# Configura el limite de mensajes sin ack para evitar crecimiento de memoria bajo carga.
	# Si ocurre un error durante la inicializacion, libera recursos
	# parciales y eleva MessageMiddlewareMessageError.
	# Si ocurre un error al liberar recursos parciales, 
	# eleva MessageMiddlewareCloseError.
	def __init__(self, host, queue_name):
		self._connection = None 
		self._channel = None
		self._queue_name = queue_name
		self._on_message_callback = None
		self._batched_publisher = None

		#Flags
		self._consuming = False
		self._consumer_tag = None
		self._consumer_queue_declared = False
		logging.getLogger("pika").setLevel(logging.WARNING) 
		try:
			self._connection = pika.BlockingConnection(rabbitmq_connection_parameters(host))
			self._channel = self._connection.channel()
			self._channel.basic_qos(prefetch_count=RABBITMQ_SETTINGS.max_unacked_messages)
			self._batched_publisher = _DestinationBatchPublisher(host, exchange_name='', declare_exchange=False)
		except Exception as e:
			self.close()
			raise MessageMiddlewareMessageError("Internal Error during initialization") from e

	# Comienza a escuchar la cola e invoca a on_message_callback por cada
	# mensaje recibido con el cuerpo del mensaje.
	# Es una operacion bloqueante hasta que se invoque stop_consuming o se
	# produzca un error.
	# on_message_callback tiene como parámetros:
	# message - El valor tal y como lo recibe el método send de esta clase.
	# ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
	# nack - Función que al invocarse realiza nack al mensaje que se está consumiendo. 
	# Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	# Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	def start_consuming(self, on_message_callback):
		try:
			self._ensure_consumer_queue_declared()
			self._on_message_callback = on_message_callback
			self._consumer_tag = self._channel.basic_consume(queue=self._queue_name,
                      on_message_callback=self._adapt_callback,
					  consumer_tag=self._consumer_tag)
			self._consuming = True
			self._channel.start_consuming()
		except (ConnectionError, pika.exceptions.AMQPConnectionError) as e:
			raise MessageMiddlewareDisconnectedError("Connection Error during start_consuming: {0}".format(str(e))) from e
		except Exception as e:
			raise MessageMiddlewareMessageError("Internal Error during start_consuming: {0}".format(str(e))) from e
		finally:
			self._on_message_callback = None
			self._consuming = False
			self._consumer_tag = None
	
	# Si no existe para esta instancia, crea una cola durable con el nombre
	# indicado en el constructor. Solo se usa para consumo.
	def _ensure_consumer_queue_declared(self):
		if self._consumer_queue_declared:
			return
		self._channel.queue_declare(queue=self._queue_name, durable=True)
		self._consumer_queue_declared = True

	# Funcion adaptadora que convierte el callback de pika al formato del
	# middleware y expone funciones de ack y nack para el mensaje actual.
	def _adapt_callback(self, ch, method, properties, body):
		_deliver_to_user_callback_transparently(
			ch,
			method,
			properties,
			body,
			self._on_message_callback,
		)

	# Si se estaba consumiendo desde la cola, detiene la escucha.
	# Si no se estaba consumiendo, no tiene efecto ni levanta error.
	# Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	def stop_consuming(self):
		if self._consuming:
			try:
				self._channel.stop_consuming(consumer_tag=self._consumer_tag)
			except (ConnectionError, pika.exceptions.AMQPConnectionError) as e:
				raise MessageMiddlewareDisconnectedError("Connection Error during stop_consuming: {0}".format(str(e))) from e
			finally:
				self._consuming = False
				self._consumer_tag = None

	# Envia un mensaje a la cola inicializada en el constructor usando el
	# exchange por defecto de RabbitMQ.
	# Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	# Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	def send(self, message):
		try:
			self._batched_publisher.enqueue(self._queue_name, message)
		except (ConnectionError, pika.exceptions.AMQPConnectionError) as e:
			raise MessageMiddlewareDisconnectedError("Connection Error during send") from e
		except Exception as e:
			raise MessageMiddlewareMessageError("Internal Error during send") from e

	# Cierra canal y conexion, intentando cerrar ambos recursos aunque uno falle.
	# Luego limpia el estado interno del middleware.
	# Si ocurre un error de cierre en algun recurso eleva MessageMiddlewareCloseError.
	def close(self):
		errors = []
		if self._batched_publisher is not None:
			try:
				self._batched_publisher.close()
			except Exception as e:
				errors.append(e)
		if self._channel is not None:
			try:
				if self._channel.is_open:
					self._channel.close()
			except Exception as e:
				errors.append(e)

		if self._connection is not None:
			try:
				if self._connection.is_open:
					self._connection.close()
			except Exception as e:
				errors.append(e)

		self._batched_publisher = None
		self._channel = None
		self._connection = None
		self._on_message_callback = None
		self._consuming = False
		self._consumer_tag = None
		self._consumer_queue_declared = False

		if errors:
			detail = "; ".join(str(e) for e in errors)
			raise MessageMiddlewareCloseError(f"Close Error: {detail}")

		
class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

	# Inicializa la conexion con RabbitMQ y el canal de comunicacion.
	# Declara el exchange directo. La cola asociada al consumidor se crea
	# lazy al iniciar start_consuming.
	# Configura el limite de mensajes sin ack para controlar memoria.
	# Si ocurre un error durante la inicializacion, libera recursos
	# parciales y eleva MessageMiddlewareMessageError.
	# Si ocurre un error al liberar recursos parciales, 
	# eleva MessageMiddlewareCloseError.
	def __init__(self, host, exchange_name, routing_keys, queue_name=None, exclusive=True):
		self._connection = None
		self._channel = None
		self._exchange_name = exchange_name
		self._routing_keys = list(routing_keys)
		self._queue_name = queue_name
		self._exclusive = exclusive
		self._on_message_callback = None
		self._batched_publisher = None

		#Flags
		self._consuming = False
		self._consumer_tag = None
		self._consumer_queue_declared = False

		try:
			self._connection = pika.BlockingConnection(rabbitmq_connection_parameters(host))
			self._channel = self._connection.channel()
			self._channel.basic_qos(prefetch_count=RABBITMQ_SETTINGS.max_unacked_messages)
			self._channel.exchange_declare(exchange=exchange_name,exchange_type='direct',durable=True)
			self._batched_publisher = _DestinationBatchPublisher(host, exchange_name=exchange_name, declare_exchange=True)
		except Exception as e:
			self.close()
			raise MessageMiddlewareMessageError("Internal Error during initialization") from e

	# Comienza a escuchar la cola asociada al exchange e invoca a
	# on_message_callback por cada mensaje recibido.
	# Es una operacion bloqueante hasta que se invoque stop_consuming o se
	# produzca un error.
	# on_message_callback tiene como parámetros:
	# message - El valor tal y como lo recibe el método send de esta clase.
	# ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
	# nack - Función que al invocarse realiza nack al mensaje que se está consumiendo. 
	# Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	# Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	def start_consuming(self, on_message_callback):
		try:
			self._declare_and_bind_queue_to_routing_keys()
			self._on_message_callback = on_message_callback
			self._consumer_tag = self._channel.basic_consume(queue=self._queue_name,
                      on_message_callback=self._adapt_callback,
					  consumer_tag=self._consumer_tag)
			self._consuming = True
			self._channel.start_consuming()
		except (ConnectionError, pika.exceptions.AMQPConnectionError) as e:
			raise MessageMiddlewareDisconnectedError("Connection Error during start_consuming: {0}".format(str(e))) from e
		except Exception as e:
			raise MessageMiddlewareMessageError("Internal Error during start_consuming: {0}".format(str(e))) from e
		finally:
			self._on_message_callback = None
			self._consuming = False
			self._consumer_tag = None

	# Funcion adaptadora que convierte el callback de pika al formato del
	# middleware y expone funciones de ack y nack para el mensaje actual.
	def _adapt_callback(self, ch, method, properties, body):
		_deliver_to_user_callback_transparently(
			ch,
			method,
			properties,
			body,
			self._on_message_callback,
		)

	# Crea una cola exclusiva autogenerada y la vincula al exchange con cada
	# routing key indicada en el constructor.
	def _declare_and_bind_queue_to_routing_keys(self):
		if self._consumer_queue_declared:
			return
		if self._queue_name is None:
			result = self._channel.queue_declare(queue='', exclusive=True)
			self._queue_name = result.method.queue
		else:
			self._channel.queue_declare(queue=self._queue_name, durable=True, exclusive=self._exclusive)
		for routing_key in self._routing_keys:
			self._channel.queue_bind(
				queue=self._queue_name,
				exchange=self._exchange_name,
				routing_key=routing_key
			)
		self._consumer_queue_declared = True

	# Si se estaba consumiendo desde el exchange, detiene la escucha.
	# Si no se estaba consumiendo, no tiene efecto ni levanta error.
	# Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	def stop_consuming(self):
		if self._consuming:
			try:
				self._channel.stop_consuming(consumer_tag=self._consumer_tag)
			except (ConnectionError, pika.exceptions.AMQPConnectionError) as e:
				raise MessageMiddlewareDisconnectedError("Connection Error during stop_consuming: {0}".format(str(e))) from e
			finally:
				self._consuming = False
				self._consumer_tag = None

	# Envia un mensaje al exchange inicializado en el constructor.
	# Publica una vez por cada routing key configurada en la instancia.
	# Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	# Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	def send(self, message):
		try:
			for routing_key in self._routing_keys:
				self._batched_publisher.enqueue(routing_key, message)
		except (ConnectionError, pika.exceptions.AMQPConnectionError) as e:
			raise MessageMiddlewareDisconnectedError("Connection Error during send: {0}".format(str(e))) from e
		except Exception as e:
			raise MessageMiddlewareMessageError("Internal Error during send: {0}".format(str(e))) from e

	# Cierra canal y conexion, intentando cerrar ambos recursos aunque uno falle.
	# Luego limpia el estado interno del middleware.
	# Si ocurre un error de cierre en algun recurso eleva MessageMiddlewareCloseError.
	def close(self):
		errors = []

		if self._batched_publisher is not None:
			try:
				self._batched_publisher.close()
			except Exception as e:
				errors.append(e)

		if self._channel is not None:
			try:
				if self._channel.is_open:
					self._channel.close()
			except Exception as e:
				errors.append(e)

		if self._connection is not None:
			try:
				if self._connection.is_open:
					self._connection.close()
			except Exception as e:
				errors.append(e)

		self._batched_publisher = None
		self._channel = None
		self._connection = None
		self._on_message_callback = None
		self._consuming = False
		self._consumer_tag = None
		self._consumer_queue_declared = False
		self._queue_name = None

		if errors:
			detail = "; ".join(str(e) for e in errors)
			raise MessageMiddlewareCloseError(f"Close Error: {detail}")


class MessageMiddlewareExchangePublisherRabbitMQ(MessageMiddlewareExchangePublisher):
	def __init__(self, host, exchange_name, bindings=None):
		self._connection = None
		self._channel = None
		self._exchange_name = exchange_name
		self._bindings = list(bindings or [])
		self._batched_publisher = None

		try:
			self._connection = pika.BlockingConnection(rabbitmq_connection_parameters(host))
			self._channel = self._connection.channel()
			self._channel.exchange_declare(
				exchange=exchange_name,
				exchange_type='direct',
				durable=True,
			)

			for queue_name, routing_key in self._bindings:
				self._channel.queue_declare(queue=queue_name, durable=True)
				self._channel.queue_bind(
					queue=queue_name,
					exchange=exchange_name,
					routing_key=routing_key,
				)

			self._batched_publisher = _DestinationBatchPublisher(
				host,
				exchange_name=exchange_name,
				declare_exchange=True,
			)

		except Exception as e:
			self.close()
			raise MessageMiddlewareMessageError("Internal Error during initialization: {0}".format(str(e))) from e

	def send(self, message, routing_key):
		try:
			self._batched_publisher.enqueue(routing_key, message)
		except (ConnectionError, pika.exceptions.AMQPConnectionError) as e:
			raise MessageMiddlewareDisconnectedError("Connection Error during send: {0}".format(str(e))) from e
		except Exception as e:
			raise MessageMiddlewareMessageError("Internal Error during send: {0}".format(str(e))) from e

	def close(self):
		errors = []

		if self._batched_publisher is not None:
			try:
				self._batched_publisher.close()
			except Exception as e:
				errors.append(e)

		if self._channel is not None:
			try:
				if self._channel.is_open:
					self._channel.close()
			except Exception as e:
				errors.append(e)

		if self._connection is not None:
			try:
				if self._connection.is_open:
					self._connection.close()
			except Exception as e:
				errors.append(e)

		self._batched_publisher = None
		self._channel = None
		self._connection = None

		if errors:
			detail = "; ".join(str(e) for e in errors)
			raise MessageMiddlewareCloseError(f"Close Error: {detail}")