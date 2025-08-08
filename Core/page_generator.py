# Core/page_generator.py
import pandas as pd
import numpy as np
import logging
import plotly.graph_objects as go

logger = logging.getLogger(__name__)

def crear_html_pagina_modelo(
    datos_modelo: pd.Series,
    df_leads: pd.DataFrame,
    figura_plotly_obj: go.Figure
):
    
    model_name_display = datos_modelo.get('Model', 'Modelo Desconocido') 
    logger.info(f"PAGE_GENERATOR: Creando página interactiva final para modelo: {model_name_display}")
    
    # --- Generación del bloque de Estadísticas ---
    median_price_val = datos_modelo.get('median_price', np.nan)
    fsr_val = datos_modelo.get('fast_selling_ratio', np.nan) 
    unique_listings_total_val = datos_modelo.get('unique_listings', 0)
    listings_latest_val = datos_modelo.get('listings_latest_date', 0)
    leads_count_val = len(df_leads) if df_leads is not None else 0

    median_price_str = f"${median_price_val:,.0f}" if pd.notna(median_price_val) else "N/A"
    fsr_str = f"{fsr_val:.2%}" if pd.notna(fsr_val) else "N/A"
    unique_listings_total_str = f"{int(unique_listings_total_val):,}"
    listings_latest_str = f"{int(listings_latest_val):,}"
    leads_count_str = f"{int(leads_count_val):,}"
    
    stats_html = f"""
    <div class="stats-card">
        <h3>{model_name_display}</h3>
        <div class="stats-grid">
            <p><strong>Precio Mediana:</strong> {median_price_str}</p>
            <p><strong>Ratio Venta Rápida:</strong> {fsr_str}</p>
            <p><strong>Anuncios Únicos:</strong> {unique_listings_total_str}</p>
            <p><strong>Anuncios Recientes:</strong> {listings_latest_str}</p>
        </div>
    </div>"""
    
    # --- Generación del HTML del Gráfico Interactivo ---
    chart_html = ""
    if figura_plotly_obj:
        # Añadir un ID al div del gráfico para que JS lo pueda encontrar
        chart_html = figura_plotly_obj.to_html(full_html=False, include_plotlyjs=False, div_id='detailChartPlotlyDiv')
    
    interactive_chart_html = f"""
    <div class="chart-card" id="detailChartContainer">
        {chart_html}
    </div>"""

    # --- Generación de la tabla de Leads con IDs únicos por fila ---
    table_body_html = ""
    if df_leads is not None and not df_leads.empty:
        # Añadir el índice como una columna para usarlo como identificador
        df_leads_indexed = df_leads.reset_index().rename(columns={'index': 'lead_index'})
        
        for _, row in df_leads_indexed.iterrows():
            # ### CAMBIO CLAVE: Se añade id y data-point-index a cada fila <tr> ###
            table_body_html += f"""
            <tr class="leads-table-row" id="lead-row-{row['lead_index']}" data-point-index="{row['lead_index']}">
                <td>{f"${row['Price']:,.0f}" if pd.notna(row['Price']) else 'N/A'}</td>
                <td>{f"{row['Kilometers']:,.0f}" if pd.notna(row['Kilometers']) else 'N/A'}</td>
                <td>{int(row['Year']) if pd.notna(row['Year']) else 'N/A'}</td>
                <td>{row['District']}</td>
                <td>{int(row['Apariciones_URL_Hist']) if pd.notna(row['Apariciones_URL_Hist']) else 'N/A'}</td>
                <td><a href="{row['URL']}" target="_blank">Ver</a></td>
            </tr>"""
    else:
        table_body_html = "<tr><td colspan='6' style='text-align:center; padding: 20px;'>No se encontraron leads para este modelo.</td></tr>"

    html_content = f"""
<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Detalle: {model_name_display}</title>
<script src="https://cdn.plot.ly/plotly-2.32.0.min.js"></script>
<style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 0; background-color: #f0f2f5; }}
    a {{ color: #007bff; text-decoration: none; font-weight: 500; }}
    a:hover {{ text-decoration: underline; }}
    
    .main-container {{ display: flex; align-items: flex-start; gap: 15px; padding: 15px; max-width: 1600px; margin: 0 auto; }}
    .left-column {{ flex: 0 0 58%; position: sticky; top: 15px; }}
    .stats-card {{ background-color: #fff; padding: 10px 20px; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.05); margin-bottom: 15px; }}
    .stats-card h3 {{ margin-top: 5px; margin-bottom: 15px; color: #1a237e; }}
    .stats-grid {{ display: flex; flex-wrap: wrap; gap: 8px 18px; }}
    .stats-grid p {{ margin: 0; font-size: 0.9em; }}
    .chart-card {{ border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}

    .right-column {{ flex: 1; min-width: 0; }}
    .leads-section {{ background-color: #fff; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.07); max-height: 92vh; overflow-y: auto; }}
    .leads-section h3 {{ position: sticky; top: 0; background: #fff; padding: 15px 20px; margin: 0; z-index: 10; border-bottom: 1px solid #dee2e6; }}
    
    table.leads-table {{ width: 100%; border-collapse: collapse; }}
    table.leads-table th {{
        position: sticky; top: 58px; background-color: #343a40; color: white; z-index: 9;
        font-size: 0.9em; text-align: center; cursor: pointer;
    }}
    table.leads-table th .sort-arrow {{ opacity: 0.5; font-size: 0.8em; margin-left: 5px; }}
    table.leads-table th, table.leads-table td {{ padding: 8px 10px; border-bottom: 1px solid #e9ecef; font-size: 0.85em; white-space: normal; }}
    table.leads-table td:nth-child(1), table.leads-table td:nth-child(2), table.leads-table td:nth-child(3), table.leads-table td:nth-child(5) {{ text-align: center; }}
    table.leads-table tr:nth-child(even) {{ background-color: #f8f9fa; }}
    
    tr.leads-table-row:hover {{ background-color: #e9ecef; }}
    tr.highlight-row {{ background-color: #cce5ff !important; transition: background-color 0.1s ease-in-out; }}

    @media (max-width: 1200px) {{
        .main-container {{ flex-direction: column; gap: 20px; }}
        .left-column {{ position: static; }}
        .leads-section {{ max-height: none; overflow-y: visible; }}
        table.leads-table th {{ position: static; }}
    }}
</style>
</head>
<body>
<div class="main-container">
    <div class="left-column">
        {stats_html}
        {interactive_chart_html}
    </div>
    <div class="right-column">
        <div class="leads-section">
            <h3>Leads Identificados ({leads_count_str})</h3>
            <table class="leads-table">
                <thead>
                    <tr>
                        <th data-column-index="0" data-column-type="numeric">Precio <span class="sort-arrow"></span></th>
                        <th data-column-index="1" data-column-type="numeric">Kilometraje <span class="sort-arrow"></span></th>
                        <th data-column-index="2" data-column-type="numeric">Año <span class="sort-arrow"></span></th>
                        <th>Distrito</th>
                        <th data-column-index="4" data-column-type="numeric">Aparic. <span class="sort-arrow"></span></th>
                        <th>Link</th>
                    </tr>
                </thead>
                <tbody id="leads-table-body">
                    {table_body_html}
                </tbody>
            </table>
        </div>
    </div>
</div>

<script>
document.addEventListener('DOMContentLoaded', function () {{
    const detailChartDiv = document.getElementById('detailChartPlotlyDiv');
    const tableBody = document.getElementById('leads-table-body');
    const tableRows = tableBody.querySelectorAll('tr.leads-table-row');
    const sortableHeaders = document.querySelectorAll('th[data-column-index]');
    let leadsTraceIndex = -1;

    if (detailChartDiv && detailChartDiv.data) {{
        leadsTraceIndex = detailChartDiv.layout.meta.leads_trace_index;
    }}
    
    // ### INICIO: LÓGICA DE INTERACTIVIDAD COMPLETA ###
    if (detailChartDiv) {{
        // Clic en un punto del gráfico para abrir URL
        detailChartDiv.on('plotly_click', function(data) {{
            const point = data.points[0];
            if (point && point.customdata && Array.isArray(point.customdata) && point.customdata.length > 0) {{
                window.open(point.customdata[0], '_blank');
            }}
        }});

        // Hover sobre un punto del gráfico para resaltar fila de la tabla
        detailChartDiv.on('plotly_hover', function(data) {{
            document.querySelectorAll('.highlight-row').forEach(row => row.classList.remove('highlight-row'));
            const point = data.points[0];
            if (point && point.customdata && Array.isArray(point.customdata) && point.customdata.length > 1) {{
                const leadIndex = point.customdata[1];
                const row = document.getElementById(`lead-row-${{leadIndex}}`);
                if (row) row.classList.add('highlight-row');
            }}
        }});

        // Limpiar resaltado cuando el cursor deja el área del gráfico
        detailChartDiv.on('plotly_unhover', function(data) {{
            document.querySelectorAll('.highlight-row').forEach(row => row.classList.remove('highlight-row'));
        }});
    }}

    // Hover sobre una fila de la tabla para resaltar punto del gráfico
    tableRows.forEach(row => {{
        row.addEventListener('mouseover', () => {{
            const pointIndex = parseInt(row.getAttribute('data-point-index'));
            if (!isNaN(pointIndex) && detailChartDiv && leadsTraceIndex !== -1) {{
                Plotly.Fx.hover(detailChartDiv, [{{ curveNumber: leadsTraceIndex, pointNumber: pointIndex }}]);
            }}
        }});
        row.addEventListener('mouseout', () => {{
            if (detailChartDiv) Plotly.Fx.hover(detailChartDiv, []);
        }});
    }});
    
    // ### INICIO: LÓGICA PARA ORDENAMIENTO DE TABLA ###
    let tableData = Array.from(tableRows).map(row => {{
        const cells = row.querySelectorAll('td');
        return {{
            rowElement: row,
            Price: parseFloat(cells[0].textContent.replace(/[^\\d.]/g, '') || 0),
            Kilometers: parseFloat(cells[1].textContent.replace(/[^\\d.]/g, '') || 0),
            Year: parseInt(cells[2].textContent || 0),
            District: cells[3].textContent,
            Apariciones: parseInt(cells[4].textContent || 0),
        }};
    }});
    
    let sortOrders = {{}};

    sortableHeaders.forEach(header => {{
        header.addEventListener('click', () => {{
            const columnIndex = parseInt(header.getAttribute('data-column-index'));
            const sortKey = Object.keys(tableData[0])[columnIndex + 1];
            
            const currentOrder = sortOrders[columnIndex] || 'desc';
            const newOrder = currentOrder === 'desc' ? 'asc' : 'desc';
            sortOrders = {{}};
            sortOrders[columnIndex] = newOrder;

            tableData.sort((a, b) => {{
                const valA = a[sortKey];
                const valB = b[sortKey];
                if (valA < valB) return newOrder === 'asc' ? -1 : 1;
                if (valA > valB) return newOrder === 'asc' ? 1 : -1;
                return 0;
            }});
            
            updateArrows(columnIndex, newOrder);
            renderTable();
        }});
    }});
    
    function updateArrows(activeColumnIndex, activeOrder) {{
        sortableHeaders.forEach(h => {{
            const arrow = h.querySelector('.sort-arrow');
            if (arrow) {{
                if (parseInt(h.getAttribute('data-column-index')) === activeColumnIndex) {{
                    arrow.innerHTML = activeOrder === 'desc' ? '▼' : '▲';
                }} else {{
                    arrow.innerHTML = '';
                }}
            }}
        }});
    }}

    function renderTable() {{
        tableData.forEach(data => tableBody.appendChild(data.rowElement));
    }}
}});
</script>
</body>
</html>"""
    
    logger.info(f"PAGE_GENERATOR: Generado HTML interactivo final (con hover recíproco) para: {model_name_display}")
    return html_content