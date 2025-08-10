# Core/main_report_builder.py
import pandas as pd
import numpy as np 
import logging
from datetime import datetime
from pathlib import Path
import json

logger = logging.getLogger(__name__)

def _get_embedded_css() -> str:
    # (El CSS no cambia)
    return """
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 0; background-color: #f0f2f5; color: #333; }
        .container { width: 95%; max-width: 1800px; margin: 20px auto; }
        header { text-align: left; margin-bottom: 20px; border-bottom: 2px solid #e0e0e0; padding-bottom: 10px;}
        .header-flex-container { display: flex; justify-content: space-between; align-items: baseline; }
        header h1 { color: #1a237e; margin: 0; font-size: 1.8em; }
        .report-info { font-size: 0.9em; color: #555; margin: 0; }
        .dashboard-container { display: flex; gap: 20px; align-items: flex-start; }
        .controls-column { flex: 0 0 280px; position: sticky; top: 20px; }
        .main-content-column { flex: 1; min-width: 0; }
        .control-panel { background-color: #fff; border-radius: 8px; padding: 20px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); }
        .control-group { margin-bottom: 25px; }
        .control-group label { font-size: 1em; font-weight: 600; margin-bottom: 10px; color: #333; display: block; }
        .control-group input[type="range"] { width: 100%; }
        .slider-value { font-weight: bold; color: #007bff; }
        #brand-filter-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
        #brand-filter-header label { margin-bottom: 0; }
        #select-all-container { font-size: 0.85em; }
        #brand-checkboxes { max-height: 350px; overflow-y: auto; border: 1px solid #ccc; border-radius: 4px; padding: 10px; display: grid; grid-template-columns: repeat(auto-fit, minmax(100px, 1fr)); gap: 8px; }
        .checkbox-item { display: flex; align-items: center; font-size: 0.9em; }
        section { margin-bottom: 30px; }
        section h2 { color: #0d47a1; margin-top: 0; padding-bottom: 10px; border-bottom: 2px solid #1976d2; }
        .chart-containerplotly { width: 100%; min-height: 650px; background-color: #fff; border-radius: 8px; padding: 10px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); }
        .styled-table { width: 100%; border-collapse: collapse; margin-top: 20px; font-size: 0.9em; }
        .styled-table th, .styled-table td { border: 1px solid #ddd; padding: 8px 10px; text-align: left; }
        .styled-table th { background-color: #343a40; color: white; }
        .styled-table tr:nth-child(even) { background-color: #f8f9fa; }
        .styled-table a { font-weight: bold; }
    </style>
    """

def generar_index_html_reporte_principal(
    fig_bubble_obj, 
    summary_opportunities_df: pd.DataFrame, 
    output_file_path: Path 
):
    logger.info("MAIN_REPORT_BUILDER: Generating dashboard with CENTRALIZED styling logic...")
    
    all_data_json = summary_opportunities_df.replace({np.nan: None}).to_json(orient='records')
    marcas_unicas = sorted(summary_opportunities_df['Make'].dropna().unique())
    
    header_html = f"""
    <header>
        <div class="header-flex-container">
            <h1>Dashboard Interactivo de Oportunidades de Mercado</h1>
            <p class="report-info">Generado el: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}</p>
        </div>
    </header>
    """
    bubble_chart_html_component = fig_bubble_obj.to_html(full_html=False, include_plotlyjs=False) if fig_bubble_obj else ""
    main_table_container = '<div id="table-container"></div>'
    
    js_logic = f"""
<script>
document.addEventListener('DOMContentLoaded', function() {{
    const allData = {all_data_json};
    const bubbleChartDiv = document.getElementById('bubbleChartDivContainer').getElementsByClassName('plotly-graph-div')[0];
    const tableContainer = document.getElementById('table-container');
    const brandCheckboxesContainer = document.getElementById('brand-checkboxes');
    const selectAllBrandsCheckbox = document.getElementById('select-all-brands');
    const fsrSlider = document.getElementById('fsr-slider');
    const listingsSlider = document.getElementById('listings-slider');
    const priceSlider = document.getElementById('price-slider');
    const fsrValueSpan = document.getElementById('fsr-value');
    const listingsValueSpan = document.getElementById('listings-value');
    const priceValueSpan = document.getElementById('price-value');
    const unicoDuenoCheckbox = document.getElementById('unico-dueno-checkbox');

    const marcas = {json.dumps(marcas_unicas)};
    marcas.forEach(marca => {{
        const div = document.createElement('div');
        div.className = 'checkbox-item';
        div.innerHTML = `<input type="checkbox" id="brand-${{marca}}" value="${{marca}}" class="brand-cb" checked> <label for="brand-${{marca}}">${{marca}}</label>`;
        brandCheckboxesContainer.appendChild(div);
    }});
    const brandCheckboxes = brandCheckboxesContainer.querySelectorAll('.brand-cb');

    function formatNumber(num) {{ return num.toLocaleString('en-US'); }}

    function handleLabelCollisions() {{
        setTimeout(() => {{
            if (!bubbleChartDiv || !bubbleChartDiv.data || !bubbleChartDiv.data[0]) return;
            const trace = bubbleChartDiv.data[0];
            if (!trace.text) return;
            const allTextNodes = Array.from(bubbleChartDiv.querySelectorAll('text'));
            const dataLabels = trace.text;
            const labelNodes = allTextNodes.filter(node => dataLabels.includes(node.textContent));
            if (labelNodes.length === 0) return;
            
            const rects = labelNodes.map(node => node.getBoundingClientRect());
            let newPositions = Array(labelNodes.length).fill('top center');
            let collisionCount = 0;
            const PADDING = 3; 

            for (let i = 0; i < rects.length; i++) {{
                for (let j = i + 1; j < rects.length; j++) {{
                    const rect1 = rects[i];
                    const rect2 = rects[j];
                    const overlap = !(rect1.right < rect2.left - PADDING || rect1.left > rect2.right + PADDING || rect1.bottom < rect2.top - PADDING || rect1.top > rect2.bottom + PADDING);
                    if (overlap && newPositions[j] === 'top center') {{
                        newPositions[j] = 'bottom center';
                        collisionCount++;
                    }}
                }}
            }}

            if (collisionCount > 0) {{
                let finalPositions = Array(trace.text.length).fill('top center');
                labelNodes.forEach((node, index) => {{
                    const dataIndex = dataLabels.indexOf(node.textContent);
                    if (dataIndex !== -1) {{
                        finalPositions[dataIndex] = newPositions[index];
                    }}
                }});
                Plotly.restyle(bubbleChartDiv, {{ 'textposition': [finalPositions] }});
            }}
        }}, 250);
    }}

    function updateDashboard() {{
        const selectedMakes = Array.from(brandCheckboxes).filter(cb => cb.checked).map(cb => cb.value);
        const minFsr = parseFloat(fsrSlider.value);
        const minListings = parseInt(listingsSlider.value);
        const maxPrice = parseFloat(priceSlider.value);
        const unicoDuenoChecked = unicoDuenoCheckbox.checked;
        
        fsrValueSpan.textContent = (minFsr * 100).toFixed(0) + '%';
        listingsValueSpan.textContent = minListings;
        priceValueSpan.textContent = '$' + formatNumber(maxPrice);
        
        const filteredData = allData.filter(d => {{
            const makeMatch = selectedMakes.length === 0 ? false : selectedMakes.includes(d.Make);
            const fsrMatch = d.fast_selling_ratio ? d.fast_selling_ratio >= minFsr : true;
            const listingsMatch = d.unique_listings >= minListings;
            const priceMatch = d.median_price ? d.median_price <= maxPrice : true;
            const unicoDuenoMatch = !unicoDuenoChecked || (unicoDuenoChecked && d.marker_border_color === 'red');
            return makeMatch && fsrMatch && listingsMatch && priceMatch && unicoDuenoMatch;
        }});

        updateBubbleChart(filteredData);
        updateTable(filteredData);
    }}

    function updateBubbleChart(data) {{
        const chartLayout = {{ ...bubbleChartDiv.layout }};
        chartLayout.title = {{ text: 'Oportunidades de Mercado (Filtros Aplicados)', font: {{ size: 20 }} }};
        if (!data || data.length === 0) {{
            Plotly.react(bubbleChartDiv, [], chartLayout);
            return;
        }}
        
        const trace = {{
            x: data.map(d => d.unique_listings), 
            y: data.map(d => d.fast_selling_ratio),
            text: data.map(d => d.Model), 
            customdata: data.map(d => d.slug),
            mode: 'markers+text', 
            textposition: 'top center',
            textfont: {{ size: 9, color: 'black' }},
            hovertemplate: "<b>%{{text}}</b><br>FSR: %{{y:.1%}}<br>Anuncios: %{{x:.0f}}<extra></extra>",
            marker: {{
                size: data.map(d => d.unique_listings),
                sizemin: 8,
                sizemode: 'area',
                sizeref: 2 * Math.max(...data.map(d => d.unique_listings || 0).map(s => s || 1)) / (45**2),
                color: data.map(d => d.marker_fill_color),
                line: {{
                    color: data.map(d => d.marker_border_color),
                    width: data.map(d => d.marker_border_width)
                }}
            }}
        }};

        Plotly.react(bubbleChartDiv, [trace], chartLayout).then(handleLabelCollisions);
    }}

    function updateTable(data) {{
        let tableHTML = '<table class="styled-table"><thead><tr><th>Modelo</th><th>FSR</th><th>Anuncios</th><th>Precio Mediana</th><th>Leads</th><th>Detalle</th></tr></thead><tbody>';
        if (data && data.length > 0) {{
            data.sort((a, b) => (b.unique_listings || 0) - (a.unique_listings || 0));
            data.forEach(d => {{
                tableHTML += `
                    <tr>
                        <td>${{d.Model || 'N/A'}}</td>
                        <td>${{d.fast_selling_ratio ? (d.fast_selling_ratio * 100).toFixed(1) + '%' : 'N/A'}}</td>
                        <td>${{d.unique_listings}}</td>
                        <td>${{d.median_price ? '$' + formatNumber(d.median_price) : 'N/A'}}</td>
                        <td>${{d.leads_count}}</td>
                        <td><a href="../model_pages/semanal/${{d.slug}}.html" target="_blank">Ver</a></td>
                    </tr>`;
            }});
        }} else {{
            tableHTML += '<tr><td colspan="6" style="text-align:center; padding:20px;">No se encontraron oportunidades con los filtros seleccionados.</td></tr>';
        }}
        tableHTML += '</tbody></table>';
        tableContainer.innerHTML = tableHTML;
    }}

    brandCheckboxes.forEach(cb => cb.addEventListener('change', updateDashboard));
    selectAllBrandsCheckbox.addEventListener('change', (e) => {{
        brandCheckboxes.forEach(cb => cb.checked = e.target.checked);
        updateDashboard();
    }});
    fsrSlider.addEventListener('input', updateDashboard);
    listingsSlider.addEventListener('input', updateDashboard);
    priceSlider.addEventListener('input', updateDashboard);
    unicoDuenoCheckbox.addEventListener('change', updateDashboard);
    
    bubbleChartDiv.on('plotly_click', function(data){{
        if(data.points.length > 0) {{
            const slug = data.points[0].customdata;
            if(slug) {{ window.open(`../model_pages/semanal/${{slug}}.html`, '_blank'); }}
        }}
    }});
    
    updateDashboard();
}});
</script>
"""
    
    full_html = f"""
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard de Oportunidades</title>
    <script src="https://cdn.plot.ly/plotly-2.32.0.min.js"></script>
    {_get_embedded_css()}
</head>
<body>
    <div class="container">
        {header_html}
        <div class="dashboard-container">
            <aside class="controls-column">
                <div class="control-panel">
                    <div class="control-group">
                        <div id="brand-filter-header">
                            <label>Marcas</label>
                            <div id="select-all-container">
                                <input type="checkbox" id="select-all-brands" checked>
                                <label for="select-all-brands"><strong>Todas</strong></label>
                            </div>
                        </div>
                        <div id="brand-checkboxes"></div>
                    </div>
                    <div class="control-group">
                        <label for="unico-dueno-checkbox">Mostrar solo con 'Único Dueño'</label>
                        <input type="checkbox" id="unico-dueno-checkbox">
                    </div>
                    <div class="control-group">
                        <label>Ratio Venta Rápida Mín: <span id="fsr-value" class="slider-value"></span></label>
                        <input type="range" id="fsr-slider" min="0" max="1" value="0" step="0.01">
                    </div>
                    <div class="control-group">
                        <label>Anuncios Mínimos: <span id="listings-value" class="slider-value"></span></label>
                        <input type="range" id="listings-slider" min="1" max="200" value="1" step="1">
                    </div>
                    <div class="control-group">
                        <label>Precio Mediano Máximo: <span id="price-value" class="slider-value"></span></label>
                        <input type="range" id="price-slider" min="5000" max="80000" value="80000" step="1000">
                    </div>
                </div>
            </aside>
            <main class="main-content-column">
                <section>
                    <div id="bubbleChartDivContainer" class="chart-containerplotly">{bubble_chart_html_component}</div>
                </section>
                <section>
                    <h2>Resumen de Modelos de Oportunidad</h2>
                    {main_table_container}
                </section>
            </main>
        </div>
    </div>
    {js_logic}
</body>
</html>"""

    try:
        output_file_path.write_text(full_html, encoding="utf-8")
        logger.info(f"MAIN_REPORT_BUILDER: Dashboard con lógica de estilo centralizada generado en: {output_file_path.resolve()}")
    except Exception as e:
        logger.error(f"MAIN_REPORT_BUILDER: Error al escribir el archivo HTML del dashboard: {e}", exc_info=True)
