import plotly.graph_objects as go
import numpy as np

# Datos de ejemplo
historic_data = {
    'Toyota Corolla': {'fsr': 0.65, 'apariciones': 1200},
    'Honda Civic': {'fsr': 0.58, 'apariciones': 950}
}

daily_leads = {
    'Toyota Corolla': {'count': 15, 'perc_unico_dueno': 0.6},
    'Honda Civic': {'count': 8, 'perc_unico_dueno': 0.4}
}

# Crear figura
fig = go.Figure()

# Capa histórica (fondo)
fig.add_trace(
    go.Scatter(
        x=[v['fsr'] for v in historic_data.values()],
        y=[v['apariciones'] for v in historic_data.values()],
        mode='markers',
        marker=dict(size=8, color='rgba(150,150,150,0.1)'),
        name='Histórico'
    )
)

# Capa de leads diarios
fig.add_trace(
    go.Scatter(
        x=[historic_data[m]['fsr'] for m in daily_leads.keys()],
        y=[historic_data[m]['apariciones'] for m in daily_leads.keys()],
        mode='markers',
        marker=dict(
            size=[d['count']*3 for d in daily_leads.values()],
            color=np.where(
                [d['perc_unico_dueno'] > 0.5 for d in daily_leads.values()],
                'green',
                'red'
            ),
            line=dict(width=1, color='black')
        ),
        name='Leads Diarios',
        customdata=np.stack((
            list(daily_leads.keys()),
            [f"{d['perc_unico_dueno']:.0%}" for d in daily_leads.values()]
        ), axis=-1),
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>"
            "FSR Histórico: %{x:.2f}<br>"
            "Apariciones Históricas: %{y:,.0f}<br>"
            "Leads Hoy: %{marker.size:,.0f}<br>"
            "Único Dueño: %{customdata[1]}<br>"
            "<extra></extra>"
        )
    )
)

# Ajustes de layout
fig.update_layout(
    xaxis_title='FSR Histórico (0-1)',
    yaxis_title='Apariciones Históricas',
    hovermode='closest',
    showlegend=True
)

# Mostrar el gráfico
fig.show()