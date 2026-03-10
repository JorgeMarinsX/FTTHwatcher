from rest_framework.routers import DefaultRouter
from .views import AcessoViewSet, TotalViewSet, DensidadeViewSet

router = DefaultRouter()
router.register("acessos", AcessoViewSet, basename="acesso")
router.register("totais", TotalViewSet, basename="total")
router.register("densidades", DensidadeViewSet, basename="densidade")

urlpatterns = router.urls
