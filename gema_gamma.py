"""
GEMA GAMMA: Servidor Cloud Receptor
Función: Escucha, valida y procesa pulsos del túnel BETA
Rol: Receptor central → Almacenamiento + Orquestación MARLETTE
Fusión: Monitoreo de acoplamiento de frecuencias por Interfaz Fácil
"""

import time
import json
import hmac
import hashlib
import lz4.frame
import numpy as np
from google.cloud import pubsub_v1
from google.cloud import firestore
from datetime import datetime
import threading
from collections import deque
from queue import Queue

class PreValidadorBiologico:
    """
    Valida integridad biológica de señales EEG recibidas e intercepta
    la asimilación de frecuencias de la Interfaz Fácil (Alpha, Theta, Beta).
    """
    
    def __init__(self):
        self.umbral_rango = 200  # µV
        self.umbral_velocidad = 50  # µV/ms (máximo cambio permitido)
        self.buffer_historico = deque(maxlen=100)
        
        # PARÁMETROS DE LA INTERFAZ FÁCIL (FUSIÓN DIGITAL-BIOLÓGICA)
        self.config_frecuencias = {
            'THETA': {'rango': (4.0, 8.0), 'color': 'Azul/Índigo', 'desfase_ms': 5.0},
            'ALPHA': {'rango': (8.0, 12.0), 'color': 'Verde/Turquesa', 'desfase_ms': 10.0},
            'BETA': {'rango': (12.0, 30.0), 'color': 'Rojo/Amarillo', 'desfase_ms': 20.0}
        }
        self.fs = 100.0  # Frecuencia de muestreo fija de Gema Alfa (100Hz)
    
    def analizar_acoplamiento_fusion(self, canales, tipo_ejercicio):
        """
        Analiza si el cerebro del individuo asimiló el desfase de milisegundos
        y entró en comunión con la frecuencia objetivo.
        """
        if tipo_ejercicio not in self.config_frecuencias:
            return {'estado_fusion': 'EJERCICIO_DESCONOCIDO', 'coherencia': 0.0}
        
        config = self.config_frecuencias[tipo_ejercicio]
        f_min, f_max = config['rango']
        
        # Calcular la Transformada Rápida de Fourier (FFT) por canal para extraer frecuencias
        n_muestras = len(canales)
        if n_muestras < 16: # Bloque mínimo requerido para análisis frecuencial básico
            return {'estado_fusion': 'DATOS_INSUFICIENTES', 'coherencia': 0.0}
            
        frecuencias = np.fft.rfftfreq(n_muestras, d=1/self.fs)
        fft_valores = np.abs(np.fft.rfft(canales, axis=0))
        
        # Encontrar la frecuencia dominante dentro del rango objetivo
        idx_rango = np.where((frecuencias >= f_min) & (frecuencias <= f_max))
        if len(idx_rango) == 0:
            return {'estado_fusion': 'FUERA_DE_RANGO_FFT', 'coherencia': 0.0}
            
        potencia_rango = np.sum(fft_valores[idx_rango, :], axis=0)
        potencia_total = np.sum(fft_valores, axis=0) + 1e-6
        
        # Proporción de la señal atrapada en la grieta de la frecuencia objetivo
        ratio_asimilacion = np.mean(potencia_rango / potencia_total)
        
        # Si la proporción supera el 35% del espectro, el cerebro asimiló el desfase
        es_sincrono = ratio_asimilacion > 0.35
        
        return {
            'estado_fusion': 'COMUNIÓN_ACTIVA' if es_sincrono else 'BUSCANDO_SINCRONÍA',
            'coherencia_asimilada': float(ratio_asimilacion),
            'frecuencia_objetivo': f"{f_min}-{f_max} Hz",
            'color_estimulo': config['color'],
            'desfase_aplicado_ms': config['desfase_ms']
        }
    
    def validar_bloque(self, bloque_datos):
        """
        Valida bloque de pulsos biológicos.
        Retorna (es_valido, detalles)
        """
        if bloque_datos is None or len(bloque_datos) == 0:
            return False, "Bloque vacío"
        
        # VALIDACIÓN 1: Rango de amplitud
        canales = bloque_datos[:, 2:]  # Columnas 2-15 = 14 canales
        max_amplitud = np.max(np.abs(canales))
        
        if max_amplitud > self.umbral_rango:
            return False, f"Amplitud fuera de rango: {max_amplitud:.1f}µV > {self.umbral_rango}µV"
        
        # VALIDACIÓN 2: Velocidad de cambio (entre muestras consecutivas)
        if len(self.buffer_historico) > 0:
            muestra_anterior = self.buffer_historico[-1]
            diferencia = np.max(np.abs(canales - muestra_anterior[2:]))
            
            if diferencia > self.umbral_velocidad:
                return False, f"Cambio brusco detectado: {diferencia:.1f}µV"
        
        # VALIDACIÓN 3: Coherencia entre canales (no deben ser idénticos / muerte cerebral o corto)
        std_canales = np.std(canales, axis=1)
        if np.min(std_canales) < 0.1:
            return False, "Canales correlacionados anormalmente"
        
        # Registrar para validación siguiente
        self.buffer_historico.append(bloque_datos)
        
        return True, "Válido biológicamente"


class ServidorGamma_Cloud:
    """
    Servidor cloud que recibe, valida y procesa pulsos EEG.
    Integra analítica de Fusión Biológica-Digital para MARLETTE.
    """
    
    def __init__(self, project_id="ecosistema-sonora-pru",
                 subscription_id="tunel-marlet-sincronia-sonora-sub"):
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.secret_key = b"HASH_SECRETO_SONORA"
        
        # FIRESTORE: Almacenamiento de sesiones
        try:
            self.db = firestore.Client(project=project_id)
        except:
            print("[GAMMA] ADVERTENCIA: Firestore no disponible (usando mock)")
            self.db = None
        
        # SUBSCRIBER: Escucha Pub/Sub
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(
            project_id, subscription_id
        )
        
        # VALIDADOR: Pre-validador Biológico
        self.validador = PreValidadorBiologico()
        
        # ESTADÍSTICAS
        self.paquetes_recibidos = 0
        self.paquetes_validos = 0
        self.paquetes_rechazados = 0
        self.errores_integridad = 0
        self.lock = threading.Lock()
        
        # BUFFER: Almacenamiento temporal
        self.cola_procesamiento = Queue(maxsize=1000)
        
        # METADATOS DE SESIÓN
        self.sesion_id = self._generar_sesion_id()
        self.inicio_sesion = datetime.now()
        
        print(f"[GAMMA] Servidor iniciado. Sesión: {self.sesion_id}")
    
    def _generar_sesion_id(self):
        """Genera ID único para la sesión."""
        timestamp = int(time.time() * 1000000)
        return f"MARLETTE-{timestamp}"
    
    def _verificar_integridad(self, datos_comprimidos, firma_recibida):
        """
        Verifica HMAC de datos recibidos.
        Detecta corrupción o manipulación en tránsito.
        """
        try:
            # Descomprimir
            datos_raw = lz4.frame.decompress(datos_comprimidos)
            
            # Separa datos de firma
            datos_payload = datos_raw[:-32]  # Todo menos los últimos 32 bytes (HMAC)
            firma_calculada = hmac.new(
                self.secret_key, 
                datos_payload, 
                hashlib.sha256
            ).digest()
            
            # Comparación segura (timing-safe)
            es_valido = hmac.compare_digest(firma_calculada, firma_recibida)
            
            if not es_valido:
                self.errores_integridad += 1
                return False, None
            
            return True, datos_payload
        
        except Exception as e:
            print(f"[GAMMA] Error de descompresión: {e}")
            return False, None
    
    def _procesar_mensaje(self, message):
        """
        Procesa mensaje recibido del túnel BETA.
        Extrae e integra la información del acoplamiento biológico.
        """
        try:
            with self.lock:
                self.paquetes_recibidos += 1
            
            # PASO 1: Verificar integridad
            datos_comprimidos = message.data
            firma_recibida = datos_comprimidos[-32:] if len(datos_comprimidos) > 32 else b''
            
            es_integro, datos_payload = self._verificar_integridad(
                datos_comprimidos, 
                firma_recibida
            )
            
            if not es_integro:
                print(f"[GAMMA] RECHAZO: Fallo de integridad en paquete {self.paquetes_recibidos}")
                with self.lock:
                    self.paquetes_rechazados += 1
                message.ack()
                return
            
            # PASO 2: Deserializar datos
            try:
                bloque = np.frombuffer(datos_payload, dtype=np.float32).reshape(-1, 16)
            except:
                print(f"[GAMMA] RECHAZO: No se pudo deserializar bloque")
                with self.lock:
                    self.paquetes_rechazados += 1
                message.ack()
                return
            
            # PASO 3: Validación Biológica Estándar
            es_valido, detalles = self.validador.validar_bloque(bloque)
            
            if not es_valido:
                print(f"[GAMMA] RECHAZO: {detalles}")
                with self.lock:
                    self.paquetes_rechazados += 1
                message.ack()
                return
            
            # PASO 4: INTERCEPCIÓN DE LA GRIETA - Asimilación de Frecuencias
            # Extraer qué tipo de ejercicio visual/binaural está corriendo el usuario desde los atributos
            tipo_ejercicio = message.attributes.get('ejercicio_tipo', 'ALPHA') 
            analisis_fusion = self.validador.analizar_acoplamiento_fusion(bloque[:, 2:], tipo_ejercicio)
            
            with self.lock:
                self.paquetes_validos += 1
            
            # Enriquecer metadatos e inyectar la información de Fusión Biológica-Digital
            metadata_paquete = {
                'sesion_id': self.sesion_id,
                'nonce': message.attributes.get('nonce', 'desconocido'),
                'timestamp_servidor': datetime.now().isoformat(),
                'timestamp_tunel': message.attributes.get('timestamp', ''),
      
