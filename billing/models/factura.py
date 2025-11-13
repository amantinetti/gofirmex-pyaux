# Python
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional, Dict, Any

@dataclass
class Factura:
    tipo: int
    folio: int
    emision: date
    vencimiento: Optional[date]
    rut: str
    razon_social: Optional[str]
    giro: Optional[str]
    direccion: Optional[str]
    comuna: Optional[str]
    neto: Optional[Decimal]
    iva: Optional[Decimal]
    total: Optional[Decimal]

    @staticmethod
    def _to_int(value: Optional[str]) -> Optional[int]:
        if value is None or value == "":
            return None
        return int(value)

    @staticmethod
    def _to_date(value: Optional[str]) -> Optional[date]:
        if value is None or value == "":
            return None
        # formato ISO: YYYY-MM-DD
        return date.fromisoformat(value)

    @staticmethod
    def _to_decimal(value: Optional[str]) -> Optional[Decimal]:
        if value is None or value == "":
            return None
        return Decimal(value)

    @classmethod
    def from_raw(cls, data: Dict[str, Any]) -> "Factura":
        """
        Crea una Factura desde un dict con valores string (por ejemplo, leídos de XML).
        Campos esperados: tipo, folio, emision, vencimiento, rut, razon_social, giro,
                          direccion, comuna, neto, iva, total
        """
        return cls(
            tipo=cls._to_int(data.get("tipo")) or 0,
            folio=cls._to_int(data.get("folio")) or 0,
            emision=cls._to_date(data.get("emision")) or date.today(),
            vencimiento=cls._to_date(data.get("vencimiento")),
            rut=(data.get("rut") or "").strip(),
            razon_social=(data.get("razon_social") or None),
            giro=(data.get("giro") or None),
            direccion=(data.get("direccion") or None),
            comuna=(data.get("comuna") or None),
            neto=cls._to_decimal(data.get("neto")),
            iva=cls._to_decimal(data.get("iva")),
            total=cls._to_decimal(data.get("total")),
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convierte a dict serializable (fechas a ISO y Decimal a str o int).
        """
        def dec_to_json(d: Optional[Decimal]) -> Optional[int]:
            if d is None:
                return None
            # Si deseas string: return str(d)
            return int(d)

        return {
            "tipo": self.tipo,
            "folio": self.folio,
            "emision": self.emision.isoformat(),
            "vencimiento": self.vencimiento.isoformat() if self.vencimiento else None,
            "rut": self.rut,
            "razon_social": self.razon_social,
            "giro": self.giro,
            "direccion": self.direccion,
            "comuna": self.comuna,
            "neto": dec_to_json(self.neto),
            "iva": dec_to_json(self.iva),
            "total": dec_to_json(self.total),
        }