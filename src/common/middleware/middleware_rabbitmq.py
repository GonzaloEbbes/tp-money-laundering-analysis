import pika
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

# Cantidad maxima de mensajes sin ack entregados al consumidor al mismo tiempo.
MAX_UNACKED_MESSAGES = 1

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

	# Inicializa la conexion con RabbitMQ y el canal de comunicacion.
	# Declara la cola durable indicada y configura el limite de mensajes
	# sin ack para evitar crecimiento de memoria bajo carga.
	# Si ocurre un error durante la inicializacion, libera recursos
	# parciales y eleva MessageMiddlewareMessageError.
	# Si ocurre un error al liberar recursos parciales, 
	# eleva MessageMiddlewareCloseError.
	def __init__(self, host, queue_name):
		self._connection = None 
		self._channel = None
		self._queue_name = queue_name
		self._on_message_callback = None

		#Flags
		self._consuming = False
		self._consumer_tag = None

		try:
			self._connection = pika.BlockingConnection(pika.ConnectionParameters(host))
			self._channel = self._connection.channel()
			self._channel.basic_qos(prefetch_count=MAX_UNACKED_MESSAGES)
			self._declare_consumer_queue()
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
			self._on_message_callback = on_message_callback
			self._consumer_tag = self._channel.basic_consume(queue=self._queue_name,
                      on_message_callback=self._adapt_callback,
					  consumer_tag=self._consumer_tag)
			self._consuming = True
			self._channel.start_consuming()
		except (ConnectionError, pika.exceptions.AMQPConnectionError) as e:
			raise MessageMiddlewareDisconnectedError("Connection Error during start_consuming") from e
		except Exception as e:
			raise MessageMiddlewareMessageError("Internal Error during start_consuming") from e
		finally:
			self._on_message_callback = None
			self._consuming = False
			self._consumer_tag = None
	
	# Si no existe, crea una cola durable con el nombre indicado en el
	# constructor. Se usa para consumo y publicacion sobre la cola.
	def _declare_consumer_queue(self):
		self._channel.queue_declare(queue=self._queue_name, durable=True)

	# Funcion adaptadora que convierte el callback de pika al formato del
	# middleware y expone funciones de ack y nack para el mensaje actual.
	def _adapt_callback(self, ch, method, properties, body):
		def ack(): ch.basic_ack(delivery_tag=method.delivery_tag)
		def nack(): ch.basic_nack(delivery_tag=method.delivery_tag)
		self._on_message_callback(body, ack, nack)

	# Si se estaba consumiendo desde la cola, detiene la escucha.
	# Si no se estaba consumiendo, no tiene efecto ni levanta error.
	# Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	def stop_consuming(self):
		if self._consuming:
			try:
				self._channel.stop_consuming(consumer_tag=self._consumer_tag)
			except (ConnectionError, pika.exceptions.AMQPConnectionError) as e:
				raise MessageMiddlewareDisconnectedError("Connection Error during stop_consuming") from e
			finally:
				self._consuming = False
				self._consumer_tag = None

	# Envia un mensaje a la cola inicializada en el constructor usando el
	# exchange por defecto de RabbitMQ.
	# Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	# Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	def send(self, message):
		try:
			self._channel.basic_publish(exchange='',routing_key=self._queue_name,body=message)
		except (ConnectionError, pika.exceptions.AMQPConnectionError) as e:
			raise MessageMiddlewareDisconnectedError("Connection Error during send") from e
		except Exception as e:
			raise MessageMiddlewareMessageError("Internal Error during send") from e

	# Cierra canal y conexion, intentando cerrar ambos recursos aunque uno falle.
	# Luego limpia el estado interno del middleware.
	# Si ocurre un error de cierre en algun recurso eleva MessageMiddlewareCloseError.
	def close(self):
		errors = []

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

		self._channel = None
		self._connection = None
		self._on_message_callback = None
		self._consuming = False
		self._consumer_tag = None

		if errors:
			detail = "; ".join(str(e) for e in errors)
			raise MessageMiddlewareCloseError(f"Close Error: {detail}")

		
class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

	# Inicializa la conexion con RabbitMQ y el canal de comunicacion.
	# Declara el exchange directo, crea una cola exclusiva y realiza bind
	# con todas las routing keys indicadas.
	# Configura el limite de mensajes sin ack para controlar memoria.
	# Si ocurre un error durante la inicializacion, libera recursos
	# parciales y eleva MessageMiddlewareMessageError.
	# Si ocurre un error al liberar recursos parciales, 
	# eleva MessageMiddlewareCloseError.
	def __init__(self, host, exchange_name, routing_keys):
		self._connection = None
		self._channel = None
		self._exchange_name = exchange_name
		self._routing_keys = list(routing_keys)
		self._queue_name = None
		self._on_message_callback = None

		#Flags
		self._consuming = False
		self._consumer_tag = None

		try:
			self._connection = pika.BlockingConnection(pika.ConnectionParameters(host))
			self._channel = self._connection.channel()
			self._channel.basic_qos(prefetch_count=MAX_UNACKED_MESSAGES)
			self._channel.exchange_declare(exchange=exchange_name,exchange_type='direct',durable=True)
			self._declare_and_bind_queue_to_routing_keys()
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
			self._on_message_callback = on_message_callback
			self._consumer_tag = self._channel.basic_consume(queue=self._queue_name,
                      on_message_callback=self._adapt_callback,
					  consumer_tag=self._consumer_tag)
			self._consuming = True
			self._channel.start_consuming()
		except (ConnectionError, pika.exceptions.AMQPConnectionError) as e:
			raise MessageMiddlewareDisconnectedError("Connection Error during start_consuming") from e
		except Exception as e:
			raise MessageMiddlewareMessageError("Internal Error during start_consuming") from e
		finally:
			self._on_message_callback = None
			self._consuming = False
			self._consumer_tag = None

	# Funcion adaptadora que convierte el callback de pika al formato del
	# middleware y expone funciones de ack y nack para el mensaje actual.
	def _adapt_callback(self, ch, method, properties, body):
		def ack(): ch.basic_ack(delivery_tag=method.delivery_tag)
		def nack(): ch.basic_nack(delivery_tag=method.delivery_tag)
		self._on_message_callback(body, ack, nack)

	# Crea una cola exclusiva autogenerada y la vincula al exchange con cada
	# routing key indicada en el constructor.
	def _declare_and_bind_queue_to_routing_keys(self):
		result = self._channel.queue_declare(queue='',exclusive=True)
		self._queue_name = result.method.queue
		for routing_key in self._routing_keys:
			self._channel.queue_bind(
				queue=self._queue_name,
				exchange=self._exchange_name,
				routing_key=routing_key
			)

	# Si se estaba consumiendo desde el exchange, detiene la escucha.
	# Si no se estaba consumiendo, no tiene efecto ni levanta error.
	# Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	def stop_consuming(self):
		if self._consuming:
			try:
				self._channel.stop_consuming(consumer_tag=self._consumer_tag)
			except (ConnectionError, pika.exceptions.AMQPConnectionError) as e:
				raise MessageMiddlewareDisconnectedError("Connection Error during stop_consuming") from e
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
				self._channel.basic_publish(exchange=self._exchange_name,routing_key=routing_key,body=message)
		except (ConnectionError, pika.exceptions.AMQPConnectionError) as e:
			raise MessageMiddlewareDisconnectedError("Connection Error during send") from e
		except Exception as e:
			raise MessageMiddlewareMessageError("Internal Error during send") from e

	# Cierra canal y conexion, intentando cerrar ambos recursos aunque uno falle.
	# Luego limpia el estado interno del middleware.
	# Si ocurre un error de cierre en algun recurso eleva MessageMiddlewareCloseError.
	def close(self):
		errors = []

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

		self._channel = None
		self._connection = None
		self._on_message_callback = None
		self._consuming = False
		self._consumer_tag = None
		self._queue_name = None

		if errors:
			detail = "; ".join(str(e) for e in errors)
			raise MessageMiddlewareCloseError(f"Close Error: {detail}")
