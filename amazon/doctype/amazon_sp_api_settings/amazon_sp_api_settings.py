# Copyright (c) 2022, Frappe and contributors
# For license information, please see license.txt


import frappe
from frappe import _
from frappe.custom.doctype.custom_field.custom_field import create_custom_fields
from frappe.model.document import Document

from ecommerce_integrations.amazon.doctype.amazon_sp_api_settings.amazon_repository import (
	get_orders,
	get_products_details,
	validate_amazon_sp_api_credentials,
	get_settlement_details
)

import datetime
from datetime import timedelta
from pytz import timezone
import pytz
class AmazonSPAPISettings(Document):
	def validate(self):
		if self.is_active == 1:
			self.validate_credentials()
			setup_custom_fields()
		else:
			self.enable_sync = 0
		if self.max_retry_limit and self.max_retry_limit > 5:
			frappe.throw(frappe._("Value for <b>Max Retry Limit</b> must be less than or equal to 5."))

	def validate_credentials(self):
		validate_amazon_sp_api_credentials(
			iam_arn=self.get("iam_arn"),
			client_id=self.get("client_id"),
			client_secret=self.get_password("client_secret"),
			refresh_token=self.get("refresh_token"),
			aws_access_key=self.get("aws_access_key"),
			aws_secret_key=self.get_password("aws_secret_key"),
			country=self.get("country"),
		)

	def after_save(self):
		if not self.is_old_data_migrated:
			migrate_old_data()
			self.db_set("is_old_data_migrated", 1)

	@frappe.whitelist()
	def get_products_details(self):
		if self.is_active == 1:
			get_products_details(amz_setting_name=self.name)

	@frappe.whitelist()
	def get_order_details(self):
		if self.is_active == 1:
			get_orders(amz_setting_name=self.name, last_updated_after=self.after_date, last_updated_before=None)


# Called via a hook in every hour.
def schedule_get_order_details():
	amz_settings = frappe.get_all(
		"Amazon SP API Settings", filters={"is_active": 1, "enable_sync": 1}, pluck="name"
	)

	for amz_setting in amz_settings:
		after_date = frappe.get_value("Amazon SP API Settings", amz_setting, "after_date")
		get_orders(amz_setting_name=amz_setting, last_updated_after=after_date, last_updated_before=None)

# Called via a hook in every 10 minutes
def schedule_get_order_details_10_mins():
	print("schedule_get_order_details_10_mins")
	amz_settings = frappe.get_all(
		"Amazon SP API Settings", filters={"is_active": 1, "enable_sync": 1}, pluck="name"
	)

	for amz_setting in amz_settings:
		after_date = frappe.get_value("Amazon SP API Settings", amz_setting, "after_date")
		print(after_date)
		swipe_in  = datetime.datetime.today()
		new_swipe_in_after = (swipe_in - timedelta(minutes=40))
		new_swipe_in_before = (swipe_in - timedelta(minutes=10))
		new_swipe_in_after = new_swipe_in_after.astimezone(timezone('US/Pacific'))
		new_swipe_in_before = new_swipe_in_before.astimezone(timezone('US/Pacific'))
		after_date = new_swipe_in_after.isoformat()
		before_date = new_swipe_in_before.isoformat()
		print(after_date)
		print(before_date)
		get_orders(amz_setting_name=amz_setting, last_updated_after=after_date, last_updated_before=before_date)

# called every Thursday at 13:30 and every Saturdat at 00:30
def get_settlement_report():
	created_until  = datetime.datetime.today()
	created_since = (created_until - timedelta(days=1))
	created_since = created_since.isoformat()
	created_until = created_until.isoformat()
	created_since = "2023-04-12T13:30"
	created_until = "2023-04-13T13:30"
	print(created_since)
	print(created_until)

	amz_settings = frappe.get_all(
		"Amazon SP API Settings", filters={"is_active": 1, "enable_sync": 1}, pluck="name"
	)
	for amz_setting in amz_settings:
		get_settlement_details(amz_setting_name=amz_setting, created_since=created_since, created_until=created_until)


def setup_custom_fields():
	custom_fields = {
		"Sales Order": [
			dict(
				fieldname="amazon_order_id",
				label="Amazon Order ID",
				fieldtype="Data",
				insert_after="title",
				read_only=1,
				print_hide=1,
			)
		],
	}

	create_custom_fields(custom_fields)


def migrate_old_data():
	column_exists = frappe.db.has_column("Item", "amazon_item_code")

	if column_exists:
		item = frappe.qb.DocType("Item")
		items = (frappe.qb.from_(item).select("*").where(item.amazon_item_code.notnull())).run(
			as_dict=True
		)

		for item in items:
			if not frappe.db.exists("Ecommerce Item", {"erpnext_item_code": item.name}):
				ecomm_item = frappe.new_doc("Ecommerce Item")
				ecomm_item.integration = "Amazon"
				ecomm_item.erpnext_item_code = item.name
				ecomm_item.integration_item_code = item.amazon_item_code
				ecomm_item.has_variants = 0
				ecomm_item.sku = item.amazon_item_code
				ecomm_item.flags.ignore_mandatory = True
				ecomm_item.save(ignore_permissions=True)
