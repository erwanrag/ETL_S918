"""
Test du système d'alerting
"""
import sys
from pathlib import Path

# Ajouter alerting au path
alerting_dir = Path(__file__).parent
sys.path.insert(0, str(alerting_dir))

from alert_manager import get_alert_manager, AlertLevel

print("🧪 Début des tests d'alerting...\n")

alert_mgr = get_alert_manager()

# Test INFO (Teams uniquement)
print("📤 Test 1/2 : Envoi notification INFO (Teams uniquement)")
alert_mgr.send_alert(
    level=AlertLevel.INFO,
    title="✅ Test - Pipeline SUCCESS",
    message="Ceci est un test de notification INFO",
    context={"Server": "S918_ETL", "Duration": "45s", "Tables": "127"}
)
print("   ✅ Envoyé\n")

# Test ERROR (Teams + Email)
print("📤 Test 2/2 : Envoi notification ERROR (Teams + Email)")
alert_mgr.send_alert(
    level=AlertLevel.ERROR,
    title="❌ Test - Pipeline FAILED",
    message="Ceci est un test d'erreur simulée pour vérifier le routage Teams + Email",
    context={
        "Server": "S918_ETL", 
        "Flow": "test-pipeline",
        "Error": "Test exception",
        "Duration": "12s"
    }
)
print("   ✅ Envoyé\n")

print("✅ Tests terminés !")
print("\n📬 Vérifiez:")
print("   - Teams : 2 notifications (INFO + ERROR)")
print("   - Email : 1 email (ERROR uniquement)")