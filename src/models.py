import datetime
import re
from sqlmodel import Field, SQLModel, create_engine
from typing import Optional
import os

class Daily_Revenue(SQLModel, table=True):
    hospital: str = Field(primary_key=True)
    date: datetime.datetime = Field(primary_key=True)
    year: int = Field(nullable=False)
    month: str = Field(nullable=False)
    day: str = Field(nullable=False)
    total_avg_invoice: Optional[float] = Field(nullable=True, default=0.00)
    total_invoices: Optional[float] = Field(nullable=True, default=0.00)
    total_gross_revenue: Optional[float] = Field(nullable=True, default=0.00)
    total_discounts: Optional[float] = Field(nullable=True, default=0.00)
    total_net_revenue: Optional[float] = Field(nullable=True, default=0.00)
    total_veterinary_services_gross_revenue: Optional[float] = Field(nullable=True, default=0.00)
    total_boarding_services_gross_revenue: Optional[float] = Field(nullable=True, default=0.00)
    total_grooming_services_gross_revenue: Optional[float] = Field(nullable=True, default=0.00)
    total_new_clients: Optional[float] = Field(nullable=True, default=0.00)
    gp_gross_revenue: Optional[float] = Field(nullable=True, default=0.00)
    gp_discounts: Optional[float] = Field(nullable=True, default=0.00)
    gp_net_revenue: Optional[float] = Field(nullable=True, default=0.00)
    gp_number_of_invoices: Optional[float] = Field(nullable=True, default=0.00)
    gp_average_invoice: Optional[float] = Field(nullable=True, default=0.00)
    gp_new_clients: Optional[float] = Field(nullable=True, default=0.00)
    hosp_gross_revenue: Optional[float] = Field(nullable=True, default=0.00)
    hosp_discounts: Optional[float] = Field(nullable=True, default=0.00)
    hosp_net_revenue: Optional[float] = Field(nullable=True, default=0.00)
    hosp_number_of_invoices: Optional[float] = Field(nullable=True, default=0.00)
    hosp_average_invoice: Optional[float] = Field(nullable=True, default=0.00)
    hosp_new_clients: Optional[float] = Field(nullable=True, default=0.00)
    im_gross_revenue: Optional[float] = Field(nullable=True, default=0.00)
    im_discounts: Optional[float] = Field(nullable=True, default=0.00)
    im_net_revenue: Optional[float] = Field(nullable=True, default=0.00)
    im_number_of_invoices: Optional[float] = Field(nullable=True, default=0.00)
    im_average_invoice: Optional[float] = Field(nullable=True, default=0.00)
    im_new_clients: Optional[float] = Field(nullable=True, default=0.00)
    surg_gross_revenue: Optional[float] = Field(nullable=True, default=0.00)
    surg_discounts: Optional[float] = Field(nullable=True, default=0.00)
    surg_net_revenue: Optional[float] = Field(nullable=True, default=0.00)
    surg_number_of_invoices: Optional[float] = Field(nullable=True, default=0.00)
    surg_average_invoice: Optional[float] = Field(nullable=True, default=0.00)
    surg_new_clients: Optional[float] = Field(nullable=True, default=0.00)
    onc_gross_revenue: Optional[float] = Field(nullable=True, default=0.00)
    onc_discounts: Optional[float] = Field(nullable=True, default=0.00)
    onc_net_revenue: Optional[float] = Field(nullable=True, default=0.00)
    onc_number_of_invoices: Optional[float] = Field(nullable=True, default=0.00)
    onc_average_invoice: Optional[float] = Field(nullable=True, default=0.00)
    onc_new_clients: Optional[float] = Field(nullable=True, default=0.00)
    er_gross_revenue: Optional[float] = Field(nullable=True, default=0.00)
    er_discounts: Optional[float] = Field(nullable=True, default=0.00)
    er_net_revenue: Optional[float] = Field(nullable=True, default=0.00)
    er_number_of_invoices: Optional[float] = Field(nullable=True, default=0.00)
    er_average_invoice: Optional[float] = Field(nullable=True, default=0.00)
    er_new_clients: Optional[float] = Field(nullable=True, default=0.00)
    total_lost_clients: Optional[float] = Field(nullable=True, default=0.00)
    total_rehab_services_gross_revenue: Optional[float] = Field(nullable=True, default=0.00)
    regional: Optional[str] = Field(nullable=True, default='')
    
    @classmethod
    def convert_values(cls, row: dict):
        for key, value in row.items():
        # remove all special characters such as $ and ,
            if isinstance(value, str) and key not in ['hospital', 'date','year', 'month', 'day']:
                row[key] = re.sub(r'[^0-9.]+', '', row[key])
                if row[key] == '':
                    row[key] = 0
        return row