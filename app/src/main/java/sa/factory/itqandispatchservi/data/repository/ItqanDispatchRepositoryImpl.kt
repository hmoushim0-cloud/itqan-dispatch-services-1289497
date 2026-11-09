package sa.factory.itqandispatchservi.data.repository

import sa.factory.itqandispatchservi.domain.repository.ItqanDispatchRepository
import sa.factory.itqandispatchservi.hardware.HardwareSensorDriver
import sa.factory.itqandispatchservi.model.ItqanDispatchItem
import sa.factory.itqandispatchservi.model.ItqanDispatchOrder
import sa.factory.itqandispatchservi.model.ItqanDispatchTelemetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch

class ItqanDispatchRepositoryImpl(
    private val sensorDriver: HardwareSensorDriver
) : ItqanDispatchRepository {

    private val _catalog = MutableStateFlow<List<ItqanDispatchItem>>(listOf(
        ItqanDispatchItem("1", "تكامل API قيود", "ربط المنصة بنظام قيود لأتمتة إنشاء الفواتير والمطابقة المحاسبية", 150.0, "معتمد", "محاسبة آلية"),
        ItqanDispatchItem("2", "تفعيل مدفوعات مادًا", "تهيئة طرفيات NFC لمدفوعات EMV L2 فورية مع مطابقة تلقائية", 280.0, "فوري", "مدفوعات NFC"),
        ItqanDispatchItem("3", "تتبع GPS للخدمات", "ربط بيانات الموقع بالمعاملات المحاسبية عبر واجهة برمجة التطبيقات", 95.0, "جديد", "تتبع مركبات")
    ))
    override val catalogFlow: Flow<List<ItqanDispatchItem>> = _catalog.asStateFlow()

    private val _cart = MutableStateFlow<List<ItqanDispatchItem>>(emptyList())
    override val activeCart: StateFlow<List<ItqanDispatchItem>> = _cart.asStateFlow()

    override val telemetryFlow: Flow<ItqanDispatchTelemetry> = sensorDriver.observePeripheralStream()
        .map { frame ->
            ItqanDispatchTelemetry(
                peripheralName = frame.peripheralName,
                status = frame.connectionState,
                liveReading = frame.livePayload,
                batteryPct = 96,
                signalDbm = -54
            )
        }

    override fun addToCart(item: ItqanDispatchItem) {
        val current = _cart.value.toMutableList()
        current.add(item)
        _cart.value = current
    }

    override fun removeFromCart(item: ItqanDispatchItem) {
        val current = _cart.value.toMutableList()
        current.remove(item)
        _cart.value = current
    }

    override fun clearCart() {
        _cart.value = emptyList()
    }

    override suspend fun checkout(paymentMethod: String): ItqanDispatchOrder {
        val currentItems = _cart.value.toList()
        val subtotal = currentItems.sumOf { it.priceSar }
        val vat = subtotal * 0.15
        val total = subtotal + vat
        val order = ItqanDispatchOrder(
            orderId = "ORD-${System.currentTimeMillis() % 100000}",
            items = currentItems,
            subtotalSar = subtotal,
            vatSar = vat,
            totalSar = total,
            paymentMethod = paymentMethod
        )
        clearCart()
        return order
    }

    override suspend fun refreshCatalog() {
        // Offline-first sync logic
    }
}
