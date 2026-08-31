from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Union
import uuid
import json
import logging

logger = logging.getLogger(__name__)

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

    @classmethod
    def from_json(cls, data: dict):
        incoming_id = data.get('Id') or data.get('id')
        return cls(
            id=str(incoming_id) if incoming_id else str(uuid.uuid4()),
            name=data.get('Name') or data.get('name', ''),
            manufacturer=data.get('Manufacturer') or data.get('manufacturer', ''),
            country_code_id=data.get('CountryCodeId') or data.get('country_code_id', ''),
            country=None,
            drug_items=[]
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
            } if self.country else None,
            'DrugItems': [item.__dict__ for item in self.drug_items],
            'Id': self.id
        }

@dataclass
class SearchQueryMessage:
    query: str
    limit: int = 5
    score_threshold: Optional[float] = None
    filters: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_message(cls, raw: Union[str, dict]) -> 'SearchQueryMessage':
        """Парсит сообщение из строки (чистый текст или JSON) или словаря"""
        if isinstance(raw, str):
            trimmed = raw.strip()
            if (trimmed.startswith('{') and trimmed.endswith('}')) or (trimmed.startswith('[') and trimmed.endswith(']')):
                try:
                    data = json.loads(trimmed)
                    if isinstance(data, dict):
                        return cls.from_dict(data)
                except Exception as e:
                    logger.warning(f"Не удалось распарсить поисковое сообщение как JSON: {trimmed[:100]}... Ошибка: {e}")
            return cls(query=trimmed)
        elif isinstance(raw, dict):
            return cls.from_dict(raw)
        else:
            return cls(query=str(raw))

    @classmethod
    def from_dict(cls, data: dict) -> 'SearchQueryMessage':
        query = data.get('query') or data.get('Query') or data.get('text') or data.get('Text')
        if not query:
            raise ValueError("Поле 'query' обязательно в JSON запросе")
        limit = data.get('limit') or data.get('Limit') or 5
        score_threshold = data.get('score_threshold') or data.get('ScoreThreshold')
        filters = data.get('filters') or data.get('Filters') or {}
        return cls(
            query=str(query),
            limit=int(limit),
            score_threshold=float(score_threshold) if score_threshold is not None else None,
            filters=filters
        )