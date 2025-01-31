# Copyright (c) 2022, Frappe and contributors
# For license information, please see license.txt

from datetime import date
import time
import urllib.request
from json import dumps
import dateutil
import os
import frappe
from frappe import _
import gzip
import json
import zlib

import ecommerce_integrations.amazon.doctype.amazon_sp_api_settings.amazon_sp_api as sp_api


from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Replace the placeholder with your Atlas connection string
uri = "mongodb+srv://amazon-sp-api:x95f6xdPsgdF5dxw@amazon-sp-api-data.ugvzbzp.mongodb.net/?retryWrites=true&w=majority"
# Set the Stable API version when creating a new client

class AmazonRepository:
	def __init__(self, amz_setting_name) -> None:
		self.amz_setting = frappe.get_doc("Amazon SP API Settings", amz_setting_name)
		self.instance_params = dict(
			iam_arn=self.amz_setting.iam_arn,
			client_id=self.amz_setting.client_id,
			client_secret=self.amz_setting.get_password("client_secret"),
			refresh_token=self.amz_setting.refresh_token,
			aws_access_key=self.amz_setting.aws_access_key,
			aws_secret_key=self.amz_setting.get_password("aws_secret_key"),
			country_code=self.amz_setting.country,
		)

	# Helper Methods
	def return_as_list(self, input):
		if isinstance(input, list):
			return input
		else:
			return [input]

	def call_sp_api_method(self, sp_api_method, **kwargs):
		errors = {}
		max_retries = self.amz_setting.max_retry_limit

		for x in range(max_retries):
			try:
				result = sp_api_method(**kwargs)
				return result.get("payload")
			except sp_api.SPAPIError as e:
				if e.error not in errors:
					errors[e.error] = e.error_description
				time.sleep(1)
				continue

		for error in errors:
			msg = f"<b>Error:</b> {error}<br/><b>Error Description:</b> {errors.get(error)}"
			frappe.msgprint(msg, alert=True, indicator="red")
			frappe.log_error(
				message=f"{error}: {errors.get(error)}", title=f'Method "{sp_api_method.__name__}" failed'
			)

		self.amz_setting.enable_sync = 0
		self.amz_setting.save()

		frappe.throw(
			_("Scheduled sync has been temporarily disabled because maximum retries have been exceeded!")
		)

	# Finances Section
	def get_finances_instance(self):
		return sp_api.Finances(**self.instance_params)

	def get_account(self, name):
		account_name = frappe.db.get_value("Account", {"account_name": "Amazon {0}".format(name)})

		if not account_name:
			new_account = frappe.new_doc("Account")
			new_account.account_name = "Amazon {0}".format(name)
			new_account.company = self.amz_setting.company
			new_account.parent_account = self.amz_setting.market_place_account_group
			new_account.insert(ignore_permissions=True)
			frappe.db.commit()
			account_name = new_account.name

		return account_name

	def get_charges_and_fees(self, order_id):
		
		finances = self.get_finances_instance()
		financial_events_payload = self.call_sp_api_method(
			sp_api_method=finances.list_financial_events_by_order_id, order_id=order_id
		)

		charges_and_fees = {"charges": [], "fees": []}
		
		while True:

			shipment_event_list = financial_events_payload.get("FinancialEvents", {}).get(
				"ShipmentEventList", []
			)
			next_token = financial_events_payload.get("NextToken")

			for shipment_event in shipment_event_list:

				if shipment_event:

					for shipment_item in shipment_event.get("ShipmentItemList", []):
						charges = shipment_item.get("ItemChargeList", [])
						fees = shipment_item.get("ItemFeeList", [])
						seller_sku = shipment_item.get("SellerSKU")

						for charge in charges:

							charge_type = charge.get("ChargeType")
							amount = charge.get("ChargeAmount", {}).get("CurrencyAmount", 0)

							if charge_type != "Principal" and float(amount) != 0:
								charge_account = self.get_account(charge_type)
								charges_and_fees.get("charges").append(
									{
										"charge_type": "Actual",
										"account_head": charge_account,
										"tax_amount": amount,
										"description": charge_type + " for " + seller_sku,
										"cost_center": "Amazon - US - CML"
									}
								)

						for fee in fees:

							fee_type = fee.get("FeeType")
							amount = fee.get("FeeAmount", {}).get("CurrencyAmount", 0)

							if float(amount) != 0:
								fee_account = self.get_account(fee_type)
								charges_and_fees.get("fees").append(
									{
										"charge_type": "Actual",
										"account_head": fee_account,
										"tax_amount": amount,
										"description": fee_type + " for " + seller_sku,
										"cost_center": "Amazon - US - CML"
									}
								)

			if not next_token:
				break

			financial_events_payload = self.call_sp_api_method(
				sp_api_method=finances.list_financial_events_by_order_id,
				order_id=order_id,
				next_token=next_token,
			)

		return charges_and_fees

	# Orders Section
	def get_orders_instance(self):
		return sp_api.Orders(**self.instance_params)

	def create_customer(self, order):
		order_customer_name = ""
		buyer_info = order.get("BuyerInfo")

		if buyer_info and buyer_info.get("BuyerName"):
			order_customer_name = buyer_info.get("BuyerName")
		else:
			order_customer_name = f"Buyer - {order.get('AmazonOrderId')}"

		existing_customer_name = frappe.db.get_value(
			"Customer", filters={"name": order_customer_name}, fieldname="name"
		)

		if existing_customer_name:
			filters = [
				["Dynamic Link", "link_doctype", "=", "Customer"],
				["Dynamic Link", "link_name", "=", existing_customer_name],
				["Dynamic Link", "parenttype", "=", "Contact"],
			]

			existing_contacts = frappe.get_list("Contact", filters)

			if not existing_contacts:
				new_contact = frappe.new_doc("Contact")
				new_contact.first_name = order_customer_name
				new_contact.append("links", {"link_doctype": "Customer", "link_name": existing_customer_name})
				new_contact.insert()

			return existing_customer_name
		else:
			new_customer = frappe.get_doc({"doctype":"Customer", "customer_name": order_customer_name, "customer_group": self.amz_setting.customer_group,
			"territory": self.amz_setting.territory, "customer_type": self.amz_setting.customer_type })
			
			new_customer.insert()
			
			new_contact = frappe.get_doc({"doctype": "Contact", "first_name": order_customer_name})

			new_contact.append("links", {"link_doctype": "Customer", "link_name": new_customer.name})

			new_contact.insert()
			
			return new_customer.name

	def create_address(self, order, customer_name):
		shipping_address = order.get("ShippingAddress")

		if not shipping_address:
			return
		else:
			address_line_1 = shipping_address.get("AddressLine1")
			if not address_line_1:
				address_line_1 = "Not Provided"
			make_address = frappe.get_doc({"doctype": "Address", "address_line1": address_line_1, "city": shipping_address.get("City"), "state": shipping_address.get("StateOrRegion"), 
			"pincode": shipping_address.get("PostalCode"), "address_type": "Shipping"})
			
			filters = [
				["Dynamic Link", "link_doctype", "=", "Customer"],
				["Dynamic Link", "link_name", "=", customer_name],
				["Dynamic Link", "parenttype", "=", "Address"],
			]
			existing_address = frappe.get_list("Address", filters)

			for address in existing_address:
				address_doc = frappe.get_doc("Address", address["name"])
				if (
					address_doc.address_line1 == make_address.address_line1
					and address_doc.pincode == make_address.pincode
				):
					return address

			make_address.append("links", {"link_doctype": "Customer", "link_name": customer_name})
			make_address.insert()
			

	def get_item_code(self, order_item):
		asin = order_item.get("ASIN")

		if asin:
			if frappe.db.exists({"doctype": "Item", "item_code": asin}):
				return asin
		else:
			raise KeyError("ASIN")

	def get_order_items(self, order_id):
		orders = self.get_orders_instance()
		order_items_payload = self.call_sp_api_method(
			sp_api_method=orders.get_order_items, order_id=order_id
		)

		final_order_items = []
		warehouse = self.amz_setting.warehouse

		while True:
			if (not order_items_payload is None):
				order_items_list = order_items_payload.get("OrderItems")
				next_token = order_items_payload.get("NextToken")

				for order_item in order_items_list:
					price = order_item.get("ItemPrice", {}).get("Amount", 0)

					final_order_items.append(
						{
							"item_code": self.get_item_code(order_item),
							"item_name": order_item.get("SellerSKU"),
							"description": order_item.get("Title"),
							"rate": price,
							"qty": order_item.get("QuantityOrdered"),
							"stock_uom": "Nos",
							"warehouse": warehouse,
							"conversion_factor": "1.0",
						}
					)

				if not next_token:
					break

				order_items_payload = self.call_sp_api_method(
					sp_api_method=orders.get_order_items, order_id=order_id, next_token=next_token
				)

		return final_order_items
	
	def create_sales_order(self, order):
	
		customer_name = self.create_customer(order)

		self.create_address(order, customer_name)

		order_id = order.get("AmazonOrderId")
		
	
		sales_order = frappe.db.get_value(
			"Sales Order", filters={"amazon_order_id": order_id}, fieldname="name"
		)

		if sales_order:
			sales_order = frappe.get_last_doc('Sales Order', filters={"amazon_order_id": order_id})
			
			if sales_order.delivery_status == "Fully Delivered" and sales_order.billing_status == "Fully Billed":
				return sales_order.name

			sales_order_invoice = frappe.db.get_value(
				"Sales Invoice", filters={"customer": customer_name}, fieldname="name"
			)

			if not sales_order_invoice:
				items = self.get_order_items(order_id)
				delivery_date = dateutil.parser.parse(order.get("LatestShipDate")).strftime("%Y-%m-%d")
				transaction_date = dateutil.parser.parse(order.get("PurchaseDate")).strftime("%Y-%m-%d")
				sales_order_invoice = frappe.get_doc(
					{
						"doctype": "Sales Invoice",
						"naming_series": "ACC-SINV-RET-.YYYY.-",
						"set_posting_time": True,
						"posting_date": transaction_date,
						"customer": customer_name,
						"due_date": delivery_date,
						"items": items
					}
				)
				sales_order_invoice.cost_center = "Amazon - US - CML"
				for item in sales_order_invoice.items:
					if not item.income_account:
						item.income_account = "431110 - Amazon US Selling price (Principal) - CML"
				sales_order_invoice.insert()
				sales_order_invoice.save()
			else:
				sales_order_invoice = frappe.get_last_doc('Sales Invoice', filters={"customer": customer_name})
				sales_order_invoice.cost_center = "Amazon - US - CML"

			order_status = order.get("OrderStatus")


			if order_status == 'Unshipped':
				sales_order.billing_status = "Not Billed"
				sales_order.delivery_status =  "Not Delivered"

			if order_status == 'PartiallyShipped':
				sales_order.billing_status = "Not Billed"
				sales_order.delivery_status =  "Partly Delivered"

			if order_status == 'Pending' or order_status == 'PendingAvailability':
				sales_order.billing_status = "Not Billed"
				sales_order.delivery_status =  "Not Delivered"

			if order_status == 'InvoiceUnconfirmed':
				sales_order.billing_status = "Not Billed"
				sales_order.delivery_status =  "Fully Delivered"
			
			if order_status == 'Unfulfillable':
				sales_order.billing_status = "Not Billed"
				sales_order.delivery_status =  "Not Delivered"

			if order_status == 'Shipped':
				sales_order.delivery_status =  "Fully Delivered"
				sales_order.per_delivered = "100"
				sales_order.per_picked = "100"
				taxes_and_charges = self.amz_setting.taxes_charges
				
				if taxes_and_charges:
					charges_and_fees = self.get_charges_and_fees(order_id)
					charges = len(charges_and_fees.get("charges"))
					feeses = len(charges_and_fees.get("fees"))
					if charges or feeses:
					
						for charge in charges_and_fees.get("charges"):
							sales_order.append("taxes", charge)
						for fee in charges_and_fees.get("fees"):
							sales_order.append("taxes", fee)
						sales_order.billing_status = "Fully Billed"
						sales_order.per_billed = "100"
			
			sales_order.save()
			sales_order_invoice.save()

			if sales_order.billing_status == "Fully Billed":
				sales_order.submit()
			
			
			return sales_order.name
		else:
			items = self.get_order_items(order_id)
			
			if(len(items) == 0):
				return "Order has no items"
			delivery_date = dateutil.parser.parse(order.get("LatestShipDate")).strftime("%Y-%m-%d")
			transaction_date = dateutil.parser.parse(order.get("PurchaseDate")).strftime("%Y-%m-%d")
			
			sales_order = frappe.get_doc(
				{
					"doctype": "Sales Order",
					"naming_series": "SAL-ORD-.YYYY.-",
					"amazon_order_id": order_id,
					"marketplace_id": order.get("MarketplaceId"),
					"customer": customer_name,
					"delivery_date": delivery_date,
					"transaction_date": transaction_date,
					"items": items,
					"company": self.amz_setting.company
				}
			)

			sales_order_invoice = frappe.get_doc(
				{
					"doctype": "Sales Invoice",
					"naming_series": "ACC-SINV-RET-.YYYY.-",
					"set_posting_time": True,
					"posting_date": transaction_date,
					"customer": customer_name,
					"due_date": delivery_date,
					"items": items
				}
			)

			sales_order_invoice.cost_center = "Amazon - US - CML"
			
			order_status = order.get("OrderStatus")
			
			if order_status == 'Unshipped':
				sales_order.billing_status = "Not Billed"
				sales_order.delivery_status =  "Not Delivered"

			if order_status == 'PartiallyShipped':
				sales_order.billing_status = "Not Billed"
				sales_order.delivery_status =  "Partly Delivered"

			if order_status == 'Pending' or order_status == 'PendingAvailability':
				sales_order.billing_status = "Not Billed"
				sales_order.delivery_status =  "Not Delivered"

			if order_status == 'InvoiceUnconfirmed':
				sales_order.billing_status = "Not Billed"
				sales_order.delivery_status =  "Fully Delivered"
			
			if order_status == 'Unfulfillable':
				sales_order.billing_status = "Not Billed"
				sales_order.delivery_status =  "Not Delivered"

			if order_status == 'Shipped':
				sales_order.delivery_status =  "Fully Delivered"
				sales_order.per_delivered = "100"
				sales_order.per_picked = "100"
				taxes_and_charges = self.amz_setting.taxes_charges
				if taxes_and_charges:
					charges_and_fees = self.get_charges_and_fees(order_id)
					charges = len(charges_and_fees.get("charges"))
					feeses = len(charges_and_fees.get("fees"))
					if charges or feeses:
					
						for charge in charges_and_fees.get("charges"):
							sales_order.append("taxes", charge)
						for fee in charges_and_fees.get("fees"):
							sales_order.append("taxes", fee)
						sales_order.billing_status = "Fully Billed"
						sales_order.per_billed = "100"
			
			sales_order.insert()
			sales_order.save()
			for item in sales_order_invoice.items:
				if not item.income_account:
					item.income_account = "431110 - Amazon US Selling price (Principal) - CML"
			sales_order_invoice.insert()
			sales_order_invoice.save()
			frappe.db.commit()

			if sales_order.billing_status == "Fully Billed":
				sales_order.submit()
			
			return sales_order.name

	def get_orders(self, last_updated_after, last_updated_before):
		orders = self.get_orders_instance()
		order_statuses = [
			"PendingAvailability",
			"Pending",
			"Unshipped",
			"PartiallyShipped",
			"Shipped",
			"InvoiceUnconfirmed",
			"Unfulfillable"
		]
		fulfillment_channels = ["FBA", "SellerFulfilled"]

		orders_payload = self.call_sp_api_method(
			sp_api_method=orders.get_orders,
			last_updated_after=last_updated_after,
			last_updated_before=last_updated_before,
			order_statuses=order_statuses,
			fulfillment_channels=fulfillment_channels,
			max_results=50,
		)
		
		sales_orders = []

		while True:

			orders_list = orders_payload.get("Orders")
			next_token = orders_payload.get("NextToken")

			if not orders_list or len(orders_list) == 0:
				break

			for order in orders_list:
				orderStatus = order.get("OrderStatus")
				if orderStatus != "Canceled":
					sales_order = self.create_sales_order(order)
					sales_orders.append(sales_order)

			if not next_token:
				break

			orders_payload = self.call_sp_api_method(
				sp_api_method=orders.get_orders, last_updated_after=last_updated_after, last_updated_before=last_updated_before, next_token=next_token
			)
			
		return sales_orders

	# CatalogItems or Products Section
	def get_catalog_items_instance(self):
		return sp_api.CatalogItems(**self.instance_params)

	def create_item_group(self, amazon_item):
		item_group_name = amazon_item.get("AttributeSets")[0].get("ProductGroup")

		if item_group_name:
			item_group = frappe.db.get_value("Item Group", filters={"item_group_name": item_group_name})

			if not item_group:
				new_item_group = frappe.new_doc("Item Group")
				new_item_group.item_group_name = item_group_name
				new_item_group.parent_item_group = self.amz_setting.parent_item_group
				new_item_group.insert()
				return new_item_group.item_group_name

			return item_group

		raise (KeyError("ProductGroup"))

	def create_brand(self, amazon_item):
		brand_name = amazon_item.get("AttributeSets")[0].get("Brand")

		if not brand_name:
			return

		existing_brand = frappe.db.get_value("Brand", filters={"brand": brand_name})

		if not existing_brand:
			brand = frappe.new_doc("Brand")
			brand.brand = brand_name
			brand.insert()
			return brand.brand
		else:
			return existing_brand

	def create_manufacturer(self, amazon_item):
		manufacturer_name = amazon_item.get("AttributeSets")[0].get("Manufacturer")

		if not manufacturer_name:
			return

		existing_manufacturer = frappe.db.get_value(
			"Manufacturer", filters={"short_name": manufacturer_name}
		)

		if not existing_manufacturer:
			manufacturer = frappe.new_doc("Manufacturer")
			manufacturer.short_name = manufacturer_name
			manufacturer.insert()
			return manufacturer.short_name
		else:
			return existing_manufacturer

	def create_ecommerce_item(self, item_code, asin, sku):
		ecommerce_item = frappe.new_doc("Ecommerce Item")
		ecommerce_item.integration = frappe.get_meta("Amazon SP API Settings").module
		ecommerce_item.erpnext_item_code = item_code
		ecommerce_item.integration_item_code = asin
		ecommerce_item.sku = sku
		ecommerce_item.insert(ignore_permissions=True)

	def create_item_price(self, amazon_item, item_code):
		item_price = frappe.new_doc("Item Price")
		item_price.price_list = self.amz_setting.price_list
		item_price.price_list_rate = (
			amazon_item.get("AttributeSets")[0].get("ListPrice", {}).get("Amount") or 0
		)
		item_price.item_code = item_code
		item_price.insert()

	def create_item(self, amazon_item, asin, sku):
		if frappe.db.get_value("Ecommerce Item", filters={"integration_item_code": asin}):
			return asin

		if frappe.db.exists("Item", asin):
			item = frappe.get_doc("Item", asin)
		else:
			# Create Item
			item = frappe.new_doc("Item")
			item.item_code = asin
			item.item_group = self.create_item_group(amazon_item)
			item.description = amazon_item.get("AttributeSets")[0].get("Title")
			item.brand = self.create_brand(amazon_item)
			item.manufacturer = self.create_manufacturer(amazon_item)
			item.image = amazon_item.get("AttributeSets")[0].get("SmallImage", {}).get("URL")
			item.insert(ignore_permissions=True)
			
			# Create Item Price
			self.create_item_price(amazon_item, item.item_code)

		# Create Ecommerce Item
		self.create_ecommerce_item(item.item_code, asin, sku)

		return item.item_code

	def get_products_details(self):
		products = []
		report_id = self.create_report()
		if report_id:
			report_document = self.get_report_document(report_id)

			if report_document:
				catalog_items = self.get_catalog_items_instance()

				for item in report_document:
					asin = item.get("asin") or item.get("product-id")
					sku = item.get("sku")
					amazon_item = catalog_items.get_catalog_item(asin=asin).get("payload")
					item_name = self.create_item(amazon_item, asin, sku)
					products.append(item_name)

		return products

	def get_settlement_details(self, created_since, created_until):

		reports = self.get_reports_instance()

		response = reports.request_reports(
			report_types=['GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE'], created_since=created_since, created_until=created_until
		)

		reportDocumentsData = []
		uniqueOrderIds = []
		print(response)
		reports = response.get("reports")
		for report in reports:
			reportId = report.get("reportId")
			reportDocumentsData, uniqueOrderIds = self.get_settlement_report_document(response=report, reportDocumentsData=reportDocumentsData, uniqueOrderIds=uniqueOrderIds)

		if(len(reportDocumentsData)):
			for order_id in uniqueOrderIds:
				order_id_settlements = list(filter(lambda item: item['order-id'] == order_id, reportDocumentsData))
				self.create_payment_entry(order_id, order_id_settlements)

	#related to invoice payment
	def create_payment_entry(self, order_id, order_id_settlements):
		
		customer_name = "Buyer - " + order_id
		sales_order_invoice_name = frappe.db.get_value(
			"Sales Invoice", filters={"customer": customer_name}, fieldname="name"
		)
		
		if(sales_order_invoice_name):
			print(sales_order_invoice_name)
			posted_date = order_id_settlements[0].get("posted-date")
			taxes_and_charges = self.get_taxes_and_charges_settlement(order_id_settlements)
			sales_order_invoice = frappe.get_last_doc('Sales Invoice', filters={"customer": customer_name})
			if(sales_order_invoice.docstatus == 0):
				for charge in taxes_and_charges.get("charges"):
					sales_order_invoice.append("taxes", charge)
				sales_order_invoice.save()
				sales_order_invoice.submit()
				frappe.db.commit()
				
				posted_date = posted_date.split("T")[0]
				contact_person = customer_name + "-" + customer_name 
				invoice_payment_entry = frappe.get_doc(
					{
						"doctype": "Payment Entry",
						"naming_series": "ACC-PAY-.YYYY.-",
						"posting_date": posted_date,
						"party_type": "Customer",
						"party": customer_name,
						"party_name": customer_name,
						"contact_person": contact_person,
						"paid_amount": sales_order_invoice.outstanding_amount,
						"received_amount": sales_order_invoice.outstanding_amount,
						"source_exchange_rate": 1,
						"target_exchange_rate": 1,
						"paid_to_account_currency": "USD",
						"paid_to": "111500 - Undeposited Funds - CML",
						"paid_from":"131100 - Accounts Receivable - USD - CML",
						"paid_from": "Debtors - CML",
						"bank_account": "Operating Account - Bank of America",
						"reference_no": sales_order_invoice_name,
						"reference_date": posted_date,
						"cost_center": "Amazon - US - CML"
					}
				)

				invoice_payment_entry.append("references", {"reference_doctype": "Sales Invoice", 
				"reference_name":  sales_order_invoice_name, 
				"total_amount": sales_order_invoice.outstanding_amount, 
				"outstanding_amount": sales_order_invoice.outstanding_amount, 
				"allocated_amount": sales_order_invoice.outstanding_amount })
				invoice_payment_entry.insert()
				invoice_payment_entry.submit()
				frappe.db.commit()

	def get_taxes_and_charges_settlement(self, order_id_settlements):
		charges_and_fees = {"charges": []}
		for settlement in order_id_settlements:

			charge_type = None
			amount = None

			if(settlement["price-type"]):
				charge_type = settlement["price-type"]
				amount = settlement["price-amount"]
			elif(settlement["item-related-fee-type"]):
				charge_type = settlement["item-related-fee-type"]
				amount = settlement["item-related-fee-amount"]
			elif(settlement["promotion-id"]):
				charge_type = settlement["promotion-id"]
				amount = settlement["promotion-amount"]
			if charge_type and charge_type != "Principal" and float(amount) != 0 and charge_type != "MarketplaceFacilitatorTax-Principal":
				seller_sku = settlement["sku"]
				charge_account = self.get_account(charge_type)
				
				charges_and_fees.get("charges").append(
					{
						"charge_type": "Actual",
						"account_head": charge_account,
						"tax_amount": amount,
						"description": charge_type + " for " + seller_sku,
						"cost_center": "Amazon - US - CML"
					}
				)
		return charges_and_fees

	def get_brand_analytics_report(self, data_start_time, data_end_time):
		
		report_id = self.create_report("GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT", data_start_time, data_end_time, {"reportPeriod": "DAY"})
		print(report_id)
		if report_id:
			report_data = self.get_report_document_brand_analytics_report(report_id)
			# write data to mongodb database
			client = MongoClient(uri, server_api=ServerApi('1'))         
			# Send a ping to confirm a successful connection
			try:
				client.admin.command('ping')
				print("Pinged your deployment. You successfully connected to MongoDB!")
				database = client["Amazon-Sp-API-Reports"]
				collection = database["Brand-Analytics-Report"]
				
				result = collection.insert_many(report_data)
				print('Brnad Analytics Data added successfully!')
			except Exception as e:
				print(e)

	# Related to Reports
	def get_reports_instance(self):
		return sp_api.Reports(**self.instance_params)

	def create_report(
		self, report_type="GET_FLAT_FILE_OPEN_LISTINGS_DATA", data_start_time=None, data_end_time=None, report_options=None
	):
		reports = self.get_reports_instance()
		
		response = reports.create_report(
			report_type=report_type, data_start_time=data_start_time, data_end_time=data_end_time, report_options=report_options
		)
		
		return response.get("reportId")

	def get_report_document(self, report_id):
		reports = self.get_reports_instance()

		for x in range(3):
			response = reports.get_report(report_id)
			processingStatus = response.get("processingStatus")

			if not processingStatus:
				raise (KeyError("processingStatus"))
			elif processingStatus in ["IN_PROGRESS", "IN_QUEUE"]:
				time.sleep(15)
				continue
			elif processingStatus in ["CANCELLED", "FATAL"]:
				raise (f"Report Processing Status: {processingStatus}")
			elif processingStatus == "DONE":
				report_document_id = response.get("reportDocumentId")

				if report_document_id:
					response = reports.get_report_document(report_document_id)
					url = response.get("url")

					if url:
						rows = []

						for line in urllib.request.urlopen(url):
							decoded_line = line.decode("utf-8").replace("\t", "\n")
							row = decoded_line.splitlines()
							rows.append(row)

						fields = rows[0]
						rows.pop(0)

						data = []

						for row in rows:
							data_row = {}
							for index, value in enumerate(row):
								data_row[fields[index]] = value
							data.append(data_row)

						return data
					raise (KeyError("url"))
				raise (KeyError("reportDocumentId"))

	def get_settlement_report_document(self, response, reportDocumentsData, uniqueOrderIds):
		reports = self.get_reports_instance()
		for x in range(3):
			processingStatus = response.get("processingStatus")

			if not processingStatus:
				raise (KeyError("processingStatus"))
			elif processingStatus in ["IN_PROGRESS", "IN_QUEUE"]:
				time.sleep(15)
				continue
			elif processingStatus in ["CANCELLED", "FATAL"]:
				raise (f"Report Processing Status: {processingStatus}")
			elif processingStatus == "DONE":
				report_document_id = response.get("reportDocumentId")
				print(report_document_id)
				if report_document_id:
					response = reports.get_report_document(report_document_id)
					url = response.get("url")
					print(url)
					counter = 0
					if url:
						rows = []
						raw_data = urllib.request.urlopen(url)
						for line in raw_data:
							decoded_line = line.decode("utf-8").replace("\t", "\n")
							row = decoded_line.splitlines()
							rows.append(row)
							
						fields = rows[0]
						rows.pop(0)

						for row in rows:
							data_row = {}
							for index, value in enumerate(row):
								data_row[fields[index]] = value
								if fields[index] == "order-id":
									if value not in uniqueOrderIds:
										uniqueOrderIds.append(value)
							reportDocumentsData.append(data_row)

						return reportDocumentsData, uniqueOrderIds
					raise (KeyError("url"))
				raise (KeyError("reportDocumentId"))

	def get_report_document_brand_analytics_report(self, report_id):
		reports = self.get_reports_instance()

		for x in range(100):
			response = reports.get_report(report_id)
			print(response)
			processingStatus = response.get("processingStatus")

			if not processingStatus:
				raise (KeyError("processingStatus"))
			elif processingStatus in ["IN_PROGRESS", "IN_QUEUE"]:
				time.sleep(15)
				continue
			elif processingStatus in ["CANCELLED", "FATAL"]:
				raise (f"Report Processing Status: {processingStatus}")
			elif processingStatus == "DONE":
				report_document_id = response.get("reportDocumentId")

				if report_document_id:
					response = reports.get_report_document(report_document_id)
					
					print(response)
					
					url = response.get("url")

					if url:
						result = urllib.request.urlopen(url)
						bytesData = result.read()
						data = gzip.decompress(bytesData)
						parsed_data = json.loads(data)
						data_date = parsed_data["reportSpecification"]["dataStartTime"]
						dataByDepartmentAndSearchTerm = parsed_data["dataByDepartmentAndSearchTerm"]
						data_length = len(dataByDepartmentAndSearchTerm)
						print(data_length)
						for i in range(data_length):
							dataByDepartmentAndSearchTerm[i]["date-stamp"] = data_date
						return dataByDepartmentAndSearchTerm
					raise (KeyError("url"))
				raise (KeyError("reportDocumentId"))


# Helper functions
def validate_amazon_sp_api_credentials(**args):
	api = sp_api.SPAPI(
		iam_arn=args.get("iam_arn"),
		client_id=args.get("client_id"),
		client_secret=args.get("client_secret"),
		refresh_token=args.get("refresh_token"),
		aws_access_key=args.get("aws_access_key"),
		aws_secret_key=args.get("aws_secret_key"),
		country_code=args.get("country"),
	)

	try:
		# validate client_id, client_secret and refresh_token.
		api.get_access_token()

		# validate aws_access_key, aws_secret_key, region and iam_arn.
		api.get_auth()

	except sp_api.SPAPIError as e:
		msg = f"<b>Error:</b> {e.error}<br/><b>Error Description:</b> {e.error_description}"
		frappe.throw(msg)


def get_orders(amz_setting_name, last_updated_after, last_updated_before):
	amazon_repository = AmazonRepository(amz_setting_name)
	return amazon_repository.get_orders(last_updated_after, last_updated_before)


def get_products_details(amz_setting_name):
	amazon_repository = AmazonRepository(amz_setting_name)
	return amazon_repository.get_products_details()

def get_brand_analytics_report(amz_setting_name, data_start_time, data_end_time):
	amazon_repository = AmazonRepository(amz_setting_name)
	return amazon_repository.get_brand_analytics_report(data_start_time, data_end_time)

def get_settlement_details(amz_setting_name, created_since, created_until):
	amazon_repository = AmazonRepository(amz_setting_name)
	return amazon_repository.get_settlement_details(created_since, created_until)
	# return amazon_repository.create_payment_entry()