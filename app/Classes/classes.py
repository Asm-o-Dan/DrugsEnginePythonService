from dataclasses import dataclass, field
from typing import List
import uuid

@dataclass
class Country:
    name: str
    code: str
    id: uuid.UUID
    drugs: List['Drug'] = field(default_factory=list)

@dataclass
class DrugItem:
    # Добавьте необходимые поля для DrugItem
    pass

@dataclass
class Drug:
    name: str
    manufacturer: str
    country_code_id: str
    country: None
    drug_items: List[DrugItem] = field(default_factory=list)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    description: str = ""
    description_source: str = "generated"

    @classmethod
    def from_json(cls, data: dict):
        # Id обязателен: точка в Qdrant должна иметь тот же идентификатор, что и препарат в PostgreSQL,
        # иначе .NET-бэкенд не сможет связать результаты поиска с каталогом.
        drug_id = data.get('Id')
        if not drug_id:
            raise ValueError("В сообщении нет Id препарата")
        return cls(
            name=data.get('Name'),
            manufacturer=data.get('Manufacturer'),
            country_code_id=data.get('CountryCodeId'),
            country=None,
            drug_items=[],
            id=str(uuid.UUID(str(drug_id))),
        )

    def to_json(self):
        return {
            'Name': self.name,
            'Manufacturer': self.manufacturer,
            'CountryCodeId': self.country_code_id,
            'Country': {
                'Name': self.country.name,
                'Code': self.country.code,
                'Drugs': self.country.drugs,
                'Id': self.country.id
            },
            'DrugItems': [item.__dict__ for item in self.drug_items],
            'Id': self.id
        }