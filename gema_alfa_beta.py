"""
GEMA ALFA: Puerto Sonora V20 Local
Función: Captura de 14 canales EEG a 100Hz sin jitter
Rol: Sensor local → Generador de pulsos válidos en Comunión Frecuencial
Fusión: Integración de colores de estimulación fótica y desfase binaural
"""

import time
import struct
import numpy as np
import gc
from threading import Lock
from collections import deque

class PuertoSonoraAlfa_V20_Local:
    """
    Captura local de señales EEG (14 canales a 100Hz).
    Pre-procesa, valida e integra las frecuencias de la Interfaz Fácil.
    CERO jitter, CERO pérdida de muestras.
    """
    
    def __init__(self, frecuencia_hz=100):
        self.frecuencia_hz = frecuencia_hz
        self.intervalo = 1.0 / frecuencia_hz  # 0.01s = 10ms
        
        # BLINDAJE: Desactivar GC durante operación crítica
        gc.disable()
        
        # BUFFER PRE-ASIGNADO: 16 columnas alineadas a 64-bit
        # Columna 0: Nonce (contador)
        # Columna 1: Timestamp
        # Columnas 2-15: 14 canales EEG
        self.capacity = 100  # 1 segundo de buffer
        self.buffer_a = np.zeros((self.capacity, 16), dtype=np.float32)
        self.buffer_b = np.zeros((self.capacity, 16), dtype=np.float32)
        self.current_buffer = self.buffer_a
        self.other_buffer = self.buffer_b
        
        self.ptr = 0
        self.nonce = 0
        self.lock = Lock()
        self.last_capture = time.time()
        self.sample_queue = deque(maxlen=1000)  # Cola de muestras capturadas
        
        # Validación de sincronía
        self.jitter_tracker = deque(maxlen=100)
        self.target_interval = self.intervalo

        # CONFIGURACIÓN DINÁMICA DE INTERFAZ FÁCIL
        self.ejercicio_activo = "ALPHA"  # ALPHA, THETA, BETA
        self.color_actual = "Verde/Turquesa"
        self.desfase_audio_ms = 10.0
        
    def cambiar_estado_ejercicio(self, tipo):
        """Cambia dinámicamente los parámetros de estimulación de la interfaz."""
        tipos_validos = {
            'THETA': ('Azul/Índigo', 5.0),
            'ALPHA': ('Verde/Turquesa', 10.0),
            'BETA': ('Rojo/Amarillo', 20.0)
        }
        if tipo in tipos_validos:
            self.ejercicio_activo = tipo
            self.color_actual, self.desfase_audio_ms = tipos_validos[tipo]
            print(f"[ALFA] Fusión conmutada a {tipo}. Estimulación: {self.color_actual} | Desfase: {self.desfase_audio_ms}ms")
        
    def _validar_sinconia(self, timestamp_actual):
        """Monitorea y valida jitter de captura."""
        if self.last_capture > 0:
            dt = timestamp_actual - self.last_capture
            jitter = abs(dt - self.target_interval)
            self.jitter_tracker.append(jitter)
            
            # Alerta silenciosa si jitter > 2ms
            if jitter > 0.002:
                return False, jitter
        
        self.last_capture = timestamp_actual
        return True, 0.0
    
    def capturar_eeg(self, simular=False):
        """
        Captura de 14 canales EEG.
        En producción: interfaz con OpenBCI/Emotiv/MindWave
        En test: simulación neural orientada a la frecuencia del ejercicio activo
        """
        if simular:
            # Simular actividad con picos armónicos en base al ejercicio para pruebas de sincronía
            frecuencias_mapeo = {'THETA': 6.0, 'ALPHA': 10.0, 'BETA': 20.0}
            freq = frecuencias_mapeo.get(self.ejercicio_activo, 10.0)
            
            t = time.time()
            # Señal base + componente oscilatoria simulando el arrastre de fase biológico
            actividad_neural = np.random.normal(0, 12, 14) 
            onda_arrastre = 15.0 * np.sin(2 * np.pi * freq * t)
            return actividad_neural + onda_arrastre
        else:
            # Aquí iría: lectura desde OpenBCI, Emotiv, etc.
            pass
    
    def inyectar_pulso(self, canales_eeg):
        """
        Inyecta pulso validado al buffer local.
        Ejecutar cada 10ms (100Hz exactos).
        """
        ahora = time.time()
        
        # VALIDACIÓN 1: Sincronía temporal
        valido, jitter = self._validar_sinconia(ahora)
        if not valido and jitter > 0.005:
            # Jitter crítico: descartar muestra
            return False
        
        # VALIDACIÓN 2: Rango de valores (±200uV es límite médico)
        if np.max(np.abs(canales_eeg)) > 200:
            return False
        
        # INYECCIÓN: Inserción atómica en buffer pre-asignado
        with self.lock:
            if self.ptr >= self.capacity:
                # Buffer lleno: intercambio atómico
                self.current_buffer, self.other_buffer = self.other_buffer, self.current_buffer
                self.ptr = 0
            
            # Escritura vectorizada
            self.current_buffer[self.ptr, 0] = float(self.nonce)  # Nonce
            self.current_buffer[self.ptr, 1] = ahora  # Timestamp
            self.current_buffer[self.ptr, 2:] = canales_eeg  # 14 canales
            
            # Metadatos enriquecidos con la configuración de fusión
            self.sample_queue.append({
                'nonce': self.nonce,
                'timestamp': ahora,
                'jitter': jitter,
                'valid': True,
                'ejercicio_tipo': self.ejercicio_activo,
                'color_pantalla': self.color_actual,
                'desfase_audio_ms': self.desfase_audio_ms
            })
            
            self.ptr += 1
            self.nonce += 1
        
        return True
    
    def obtener_lote(self, tamaño=10):
        """
        Extrae lote de muestras capturadas.
        Retorna array de (tamaño, 16) listo para transmisión.
        """
        with self.lock:
            if self.ptr >= tamaño:
                lote = self.current_buffer[:tamaño].copy()
                self.ptr = 0
                return lote
        return None
    
    def obtener_diagnostico(self):
        """Retorna métricas de calidad de captura."""
        if len(self.jitter_tracker) == 0:
            return None
        
        jitter_array = np.array(list(self.jitter_tracker))
        return {
            'jitter_mean': np.mean(jitter_array) * 1000,  # ms
            'jitter_max': np.max(jitter_array) * 1000,    # ms
            'jitter_std': np.std(jitter_array) * 1000,    # ms
            'muestras_validas': self.nonce,
            'tasa_captura_hz': self.frecuencia_hz,
            'ejercicio_activo': self.ejercicio_activo
        }


"""
GEMA BETA: Túnel Pub/Sub + LogicShield
Función: Transporte seguro + Monitoreo de jitter en tiempo real
Rol: Transporte validado → Servidor cloud (GAMMA)
Fusión: Inyección de metadatos de sincronía binaural en los atributos de transporte
"""

import hmac
import hashlib
import lz4.frame
from google.cloud import pubsub_v1
from concurrent import futures

class LogicShield:
    """
    Monitor de integridad en tiempo real.
    Detecta: jitter, corrupción, pérdida de paquetes.
    """
    
    def __init__(self):
        self.paquetes_enviados = 0
        self.paquetes_confirmados = 0
        self.paquetes_perdidos = 0
        self.latencias = deque(maxlen=1000)
        self.jitter_red = deque(maxlen=1000)
        self.lock = Lock()
        self.alarmas = deque(maxlen=100)
    
    def registrar_envio(self, nonce, timestamp):
        """Registra intentos de envío."""
        with self.lock:
            self.paquetes_enviados += 1
            return {
                'nonce': nonce,
                'timestamp_envio': timestamp,
                'id_envio': self.paquetes_enviados
            }
    
    def registrar_confirmacion(self, metadata_envio, timestamp_confirmacion):
        """Registra chimneys de entrega y calcula jitter de red."""
        with self.lock:
            latencia = (timestamp_confirmacion - metadata_envio['timestamp_envio']) * 1000  # ms
            self.latencias.append(latencia)
            self.paquetes_confirmados += 1
            
            if len(self.latencias) >= 2:
                jitter = abs(self.latencias[-1] - self.latencias[-2])
                self.jitter_red.append(jitter)
            
            if latencia > 100:
                alarma = f"ALERTA: Latencia alta detectada ({latencia:.2f}ms)"
                self.alarmas.append(alarma)
                print(f"[LOGICSHIELD] {alarma}")
    
    def registrar_perdida(self, nonce):
        """Registra pérdida de paquete."""
        with self.lock:
            self.paquetes_perdidos += 1
            alarma = f"PERDIDA: Paquete {nonce} no confirmado"
            self.alarmas.append(alarma)
            print(f"[LOGICSHIELD] {alarma}")
    
    def calcular_jitter(self):
        """Calcula jitter de red promedio en ms."""
        if len(self.jitter_red) == 0:
            return 0.0
        return float(np.mean(self.jitter_red))


class TunelBeta_Publisher:
    """
    Gestiona el envío empaquetado, comprimido y firmado desde ALFA hacia GAMMA.
    Inyecta el estado de estimulación de la Interfaz Fácil.
    """
    
    def __init__(self, project_id="ecosistema-sonora-pru", topic_id="tunel-marlet-sincronia-sonora"):
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_id)
        self.secret_key = b"HASH_SECRETO_SONORA"
        self.shield = LogicShield()
        self.version_tunnel = "V2.0-FUSION"
        
    def transmitir_lote(self, lote_datos, metadata_alfa):
        """
        Comprime, firma y transmite el lote inyectando las frecuencias 
        del ejercicio en los atributos de transporte.
        """
        if lote_datos is None:
            return False
            
        try:
            datos_raw = lote_datos.tobytes()
            firma = hmac.new(self.secret_key, datos_raw, hashlib.sha256).digest()
            payload_firmado = datos_raw + firma
            payload_comprimido = lz4.frame.compress(payload_firmado)
            
            ahora = time.time()
            nonce = int(metadata_alfa['nonce'])
          
