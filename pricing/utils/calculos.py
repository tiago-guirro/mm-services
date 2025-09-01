"""Cálculos de preço com arredondamento específico"""
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN

def round_up(value: Decimal) -> Decimal:
    """
    Arredonda o valor para 4 casas decimais com arredondamento tradicional (meio para cima).
    Usado para manter precisão em cálculos intermediários de preço.
    """
    return value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

def round_salles(value: Decimal) -> Decimal:
    """
    Arredonda o valor para baixo até o inteiro mais próximo e adiciona 0.90.
    Exemplo: 13.78 → 13.00 + 0.90 = 13.90.
    Usado para ajustar o preço final com terminação comercial padrão.
    """
    return value.to_integral_value(rounding=ROUND_DOWN) + Decimal("0.90")

def round_two(value: Decimal) -> Decimal:
    """
    Arredonda o valor para 1 casas decimais com arredondamento tradicional (meio para cima).
    Usado para manter precisão em cálculos intermediários de preço.
    """
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
