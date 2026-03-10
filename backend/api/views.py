from rest_framework import mixins, viewsets
from .models import Acesso, Total, Densidade
from .serializers import AcessoSerializer, TotalSerializer, DensidadeSerializer
from .filters import AcessoFilter, DensidadeFilter


class AcessoViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Acesso.objects.all()
    serializer_class = AcessoSerializer
    filterset_class = AcessoFilter
    ordering_fields = ["ano", "mes", "uf", "municipio", "acessos"]


class TotalViewSet(mixins.ListModelMixin, viewsets.GenericViewSet):
    # totais has a composite PK — list only, no retrieve.
    queryset = Total.objects.all()
    serializer_class = TotalSerializer
    filterset_fields = ["ano", "mes"]
    ordering_fields = ["ano", "mes"]


class DensidadeViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Densidade.objects.all()
    serializer_class = DensidadeSerializer
    filterset_class = DensidadeFilter
    ordering_fields = ["ano", "mes", "uf", "densidade"]
