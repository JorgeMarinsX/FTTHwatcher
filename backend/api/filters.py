import django_filters
from .models import Acesso, Densidade


class AcessoFilter(django_filters.FilterSet):
    ano = django_filters.NumberFilter()
    ano__gte = django_filters.NumberFilter(field_name="ano", lookup_expr="gte")
    ano__lte = django_filters.NumberFilter(field_name="ano", lookup_expr="lte")
    mes = django_filters.NumberFilter()
    uf = django_filters.CharFilter(lookup_expr="iexact")
    cnpj = django_filters.CharFilter()
    ibge = django_filters.NumberFilter()
    tecnologia = django_filters.CharFilter(lookup_expr="icontains")
    meio_acesso = django_filters.CharFilter(lookup_expr="icontains")
    empresa = django_filters.CharFilter(lookup_expr="icontains")
    grupo_economico = django_filters.CharFilter(lookup_expr="icontains")
    tipo_pessoa = django_filters.CharFilter(lookup_expr="iexact")
    tipo_produto = django_filters.CharFilter(lookup_expr="iexact")

    class Meta:
        model = Acesso
        fields = [
            "ano", "mes", "uf", "cnpj", "ibge",
            "tecnologia", "meio_acesso", "empresa",
            "grupo_economico", "tipo_pessoa", "tipo_produto",
        ]


class DensidadeFilter(django_filters.FilterSet):
    ano = django_filters.NumberFilter()
    mes = django_filters.NumberFilter()
    uf = django_filters.CharFilter(lookup_expr="iexact")
    nivel = django_filters.CharFilter(lookup_expr="iexact")
    ibge = django_filters.NumberFilter()

    class Meta:
        model = Densidade
        fields = ["ano", "mes", "uf", "nivel", "ibge"]
