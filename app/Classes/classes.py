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

    form: str | None = None
    active_ingredient: str | None = None
    description: str | None = None
    description_source: str | None = None

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
            form=data.get('Form'),
            active_ingredient=data.get('ActiveIngredient'),
        )

    def search_text(self) -> str:
        """Текст для эмбеддинга: описание (если есть) плюс то, что известно из каталога."""
        parts = [self.name]
        if self.active_ingredient:
            parts.append(f"Действующее вещество: {self.active_ingredient}")
        if self.form:
            parts.append(f"Форма выпуска: {self.form}")
        if self.description:
            parts.append(self.description)
        return ". ".join(p for p in parts if p)

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