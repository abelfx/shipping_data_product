from typing import List, Optional
from datetime import datetime, date
from pydantic import BaseModel, Field


class TopProductItem(BaseModel):
	term: str = Field(description="Token/term from message text")
	count: int = Field(description="Frequency across all messages")


class ChannelActivityItem(BaseModel):
	date: date
	posts: int
	avg_views: float
	avg_forwards: float


class MessageItem(BaseModel):
	message_id: int
	channel_name: str
	date: datetime
	message_text: str
	views: int
	forward_count: int
	has_image: bool


class VisualContentStatsItem(BaseModel):
	channel_name: str
	total_images: int
	promotional_images: int
	product_display_images: int
	lifestyle_images: int
	other_images: int


class TopProductsResponse(BaseModel):
	items: List[TopProductItem]


class ChannelActivityResponse(BaseModel):
	channel_name: str
	items: List[ChannelActivityItem]


class MessageSearchResponse(BaseModel):
	items: List[MessageItem]


class VisualContentStatsResponse(BaseModel):
	items: List[VisualContentStatsItem]
