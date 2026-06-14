#!/bin/bash

echo "=============================================================="
echo "    INICIALIZANDO ENTORNO SOBERANO - INTERFAZ FÁCIL (MARLETTE)"
echo "=============================================================="

# 1. DESVÍO DE SEGURIDAD: Redirección de flujos a entornos Locales (Sandbox)
export PUBSUB_EMULATOR_HOST="localhost:8085"
export FIRESTORE_EMULATOR_HOST="localhost:8080"
export GOOGLE_CLOUD_PROJECT="ecosistema-sonora-pru"

echo "[CONFIG] Variables de entorno inyectadas con éxito."
echo "  -> Pub/Sub local apuntando a: $PUBSUB_EMULATOR_HOST"
echo "  -> Firestore local apuntando a: $FIRESTORE_EMULATOR_HOST"
echo "  -> Proyecto activo: $GOOGLE_CLOUD_PROJECT"
echo "=============================================================="

# 2. INSTRUCCIÓN PARA LAS DOS FORMAS DEL TÚNEL SOBERANO
echo "[MODO SIMULACIÓN LOCAL ACTIVO]"
echo "Forma 1: Emulador local Cortex -> GEMA ALFA -> GEMA BETA -> Emuladores Google"
echo "Forma 2: Entrada directa por tubería de datos alternativa hacia GEMA GAMMA"
echo "=============================================================="

# 3. Lanzar una prueba rápida del circuito local unificado en Python
echo "[EJECUCIÓN] Iniciando validación de pulsos en gema_alfa_beta.py..."
python gema_alfa_beta.py

echo "=============================================================="
echo "Entorno configurado. Listo para recibir la inyección del túnel."

