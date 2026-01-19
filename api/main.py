import os
from typing import List

from fastapi import FastAPI, Depends, Query
from sqlalchemy.engine import Connection
from sqlalchemy import text

from .database import get_connection
from .schemas import (
	TopProductItem,
	TopProductsResponse,
	ChannelActivityItem,
	ChannelActivityResponse,
	MessageItem,
	MessageSearchResponse,
	VisualContentStatsItem,
	VisualContentStatsResponse,
)

app = FastAPI(title="Medical Warehouse Analytics API", version="1.0.0")

# Default schema used by dbt (from profiles.yml); adjust via env if needed
DBT_SCHEMA = os.getenv("DBT_SCHEMA", "staging")


@app.get("/api/reports/top-products", response_model=TopProductsResponse, tags=["Reports"], summary="Top frequently mentioned terms/products")
def top_products(limit: int = Query(10, ge=1, le=100), conn: Connection = Depends(get_connection)):
	# Simple tokenization using Postgres; filter short tokens and common stopwords
	stopwords = "the a an and or to for of in on with at by from is are was were be been being this that these those"
	query = text(f"""
		with tokens as (
			select lower(t) as term
			from {DBT_SCHEMA}.fct_message m,
				 regexp_split_to_table(m.message_text, '\\W+') t
		)
		select term, count(*) as count
		from tokens
		where length(term) >= 3
		  and term not in (select unnest(string_to_array(:stopwords, ' ')))
		group by term
		order by count desc
		limit :limit
	""")
	rows = conn.execute(query, {"limit": limit, "stopwords": stopwords}).fetchall()
	items = [TopProductItem(term=r[0], count=r[1]) for r in rows]
	return TopProductsResponse(items=items)


@app.get("/api/channels/{channel_name}/activity", response_model=ChannelActivityResponse, tags=["Channels"], summary="Daily activity and engagement for a channel")
def channel_activity(channel_name: str, conn: Connection = Depends(get_connection)):
	query = text(f"""
		select
			date_trunc('day', m.date_key)::date as date,
			count(*) as posts,
			avg(m.views)::float as avg_views,
			avg(m.forward_count)::float as avg_forwards
		from {DBT_SCHEMA}.fct_message m
		join {DBT_SCHEMA}.dim_channels c on m.channel_key = c.channel_key
		where c.channel_name = :channel_name
		group by date
		order by date asc
	""")
	rows = conn.execute(query, {"channel_name": channel_name}).fetchall()
	items = [ChannelActivityItem(date=r[0], posts=r[1], avg_views=r[2], avg_forwards=r[3]) for r in rows]
	return ChannelActivityResponse(channel_name=channel_name, items=items)


@app.get("/api/search/messages", response_model=MessageSearchResponse, tags=["Search"], summary="Search messages by keyword")
def search_messages(query: str = Query(..., min_length=2), limit: int = Query(20, ge=1, le=100), conn: Connection = Depends(get_connection)):
	sql = text(f"""
		select m.message_id, c.channel_name, m.date_key, m.message_text, m.views, m.forward_count, m.has_image
		from {DBT_SCHEMA}.fct_message m
		join {DBT_SCHEMA}.dim_channels c on m.channel_key = c.channel_key
		where m.message_text ilike :pattern
		order by m.date_key desc
		limit :limit
	""")
	rows = conn.execute(sql, {"pattern": f"%{query}%", "limit": limit}).fetchall()
	items = [
		MessageItem(
			message_id=r[0], channel_name=r[1], date=r[2], message_text=r[3], views=r[4], forward_count=r[5], has_image=r[6]
		)
		for r in rows
	]
	return MessageSearchResponse(items=items)


@app.get("/api/reports/visual-content", response_model=VisualContentStatsResponse, tags=["Reports"], summary="Visual content stats across channels")
def visual_content_stats(conn: Connection = Depends(get_connection)):
	# Prefer image category from detections if available; fallback to has_image flag
	sql = text(f"""
		with base as (
			select c.channel_name,
				   coalesce(d.image_category, case when m.has_image then 'unknown' else null end) as image_category
			from {DBT_SCHEMA}.fct_message m
			join {DBT_SCHEMA}.dim_channels c on m.channel_key = c.channel_key
			left join {DBT_SCHEMA}.fct_image_detections d on d.message_id = m.message_id
			where m.has_image = true
		)
		select channel_name,
			   count(*) as total_images,
			   sum(case when image_category = 'promotional' then 1 else 0 end) as promotional_images,
			   sum(case when image_category = 'product_display' then 1 else 0 end) as product_display_images,
			   sum(case when image_category = 'lifestyle' then 1 else 0 end) as lifestyle_images,
			   sum(case when image_category = 'other' then 1 else 0 end) as other_images
		from base
		group by channel_name
		order by total_images desc
	""")
	rows = conn.execute(sql).fetchall()
	items = [
		VisualContentStatsItem(
			channel_name=r[0], total_images=r[1], promotional_images=r[2], product_display_images=r[3], lifestyle_images=r[4], other_images=r[5]
		)
		for r in rows
	]
	return VisualContentStatsResponse(items=items)


# Root & docs helpers
@app.get("/", summary="Health check")
def root():
	return {"status": "ok"}
