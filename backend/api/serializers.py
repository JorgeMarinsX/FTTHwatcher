from rest_framework import serializers
from .models import Acesso, Total, Densidade


class AcessoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Acesso
        fields = "__all__"


class TotalSerializer(serializers.ModelSerializer):
    class Meta:
        model = Total
        fields = "__all__"


class DensidadeSerializer(serializers.ModelSerializer):
    class Meta:
        model = Densidade
        fields = "__all__"
