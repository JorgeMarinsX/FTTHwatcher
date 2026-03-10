from django.db import models
from django.db.models import CompositePrimaryKey


class Acesso(models.Model):
    ano = models.SmallIntegerField()
    mes = models.SmallIntegerField()
    grupo_economico = models.TextField(null=True, blank=True)
    empresa = models.TextField()
    cnpj = models.CharField(max_length=14)
    porte = models.TextField(null=True, blank=True)
    uf = models.CharField(max_length=2)
    municipio = models.TextField()
    ibge = models.IntegerField(null=True, blank=True)
    faixa_velocidade = models.TextField(null=True, blank=True)
    velocidade_mbps = models.DecimalField(max_digits=12, decimal_places=6, null=True, blank=True)
    tecnologia = models.TextField(null=True, blank=True)
    meio_acesso = models.TextField(null=True, blank=True)
    tipo_pessoa = models.TextField(null=True, blank=True)
    tipo_produto = models.TextField(null=True, blank=True)
    acessos = models.IntegerField()
    fonte = models.TextField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = "acessos"
        ordering = ["ano", "mes"]


class Total(models.Model):
    pk = CompositePrimaryKey("ano", "mes")
    ano = models.SmallIntegerField()
    mes = models.SmallIntegerField()
    acessos = models.BigIntegerField()

    class Meta:
        managed = False
        db_table = "totais"
        ordering = ["ano", "mes"]


class Densidade(models.Model):
    ano = models.SmallIntegerField()
    mes = models.SmallIntegerField()
    uf = models.TextField(null=True, blank=True)
    municipio = models.TextField(null=True, blank=True)
    ibge = models.IntegerField(null=True, blank=True)
    densidade = models.DecimalField(max_digits=18, decimal_places=12, null=True, blank=True)
    nivel = models.TextField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = "densidades"
        ordering = ["ano", "mes"]
