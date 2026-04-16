# visualization/dashboard.py
"""
Interactive Visualization Dashboard
"""

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np

class YouTubeDashboard:
    def __init__(self):
        self.fig = None
        
    def create_viral_factors_chart(self, viral_data):
        """
        Visualization for viral factors analysis
        Justification: Radar chart shows multi-dimensional comparison effectively
        """
        df = viral_data.toPandas()
        
        fig = go.Figure()
        
        categories = ['Title Length', 'Tag Count', 'Engagement', 'Caps Usage', 'Punctuation']
        
        for viral_cat in df['viral_category'].unique():
            subset = df[df['viral_category'] == viral_cat].iloc[0]
            
            values = [
                subset['avg_title_length'] / 100,  # Normalize
                subset['avg_tag_count'] / 50,
                subset['avg_engagement'] * 100,
                subset['has_caps_ratio'],
                subset['has_punctuation_ratio']
            ]
            
            fig.add_trace(go.Scatterpolar(
                r=values,
                theta=categories,
                fill='toself',
                name=viral_cat
            ))
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, 1]
                )),
            showlegend=True,
            title="Viral Video Characteristics Analysis"
        )
        
        return fig
    
    def create_optimal_timing_heatmap(self, timing_data):
        """
        Heatmap for optimal posting times
        Justification: Heatmap clearly shows time patterns across 2 dimensions
        """
        df = timing_data.toPandas()
        
        # Pivot for heatmap
        pivot_views = df.pivot(index='day_of_week', columns='hour', values='global_avg_views')
        
        fig = px.imshow(
            pivot_views,
            labels=dict(x="Hour of Day", y="Day of Week", color="Average Views"),
            x=list(range(24)),
            y=['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
            color_continuous_scale='RdYlBu_r',
            title="Optimal Posting Time Heatmap"
        )
        
        fig.update_xaxis(dtick=1)
        
        return fig
    
    def create_category_evolution_chart(self, evolution_data):
        """
        Time series chart for category evolution
        Justification: Line chart best shows trends over time
        """
        df = evolution_data.toPandas()
        
        # Get top 5 categories
        top_categories = df.groupby('category_id')['total_views'].sum().nlargest(5).index
        df_filtered = df[df['category_id'].isin(top_categories)]
        
        fig = px.line(
            df_filtered,
            x='month',
            y='total_views',
            color='category_id',
            title="Category Popularity Evolution",
            labels={'total_views': 'Total Views', 'month': 'Month'}
        )
        
        fig.update_layout(
            hovermode='x unified',
            xaxis_tickangle=-45
        )
        
        return fig
    
    def create_channel_strategy_chart(self, strategy_data):
        """
        Grouped bar chart for channel strategies
        Justification: Grouped bars allow comparison across multiple metrics
        """
        df = strategy_data.toPandas()
        
        fig = go.Figure()
        
        metrics = ['avg_category_diversity', 'avg_consistency', 'avg_peak_ratio']
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1']
        
        for i, metric in enumerate(metrics):
            fig.add_trace(go.Bar(
                name=metric.replace('avg_', '').replace('_', ' ').title(),
                x=df['performance_tier'],
                y=df[metric],
                marker_color=colors[i]
            ))
        
        fig.update_layout(
            barmode='group',
            title="Channel Performance Strategies",
            xaxis_title="Performance Tier",
            yaxis_title="Metric Value",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        return fig
    
    def create_comprehensive_dashboard(self, viral_data, timing_data, evolution_data, strategy_data):
        """
        Create comprehensive dashboard with all visualizations
        """
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Viral Factors', 'Optimal Timing', 
                          'Category Evolution', 'Channel Strategies'),
            specs=[[{'type': 'polar'}, {'type': 'heatmap'}],
                   [{'type': 'scatter'}, {'type': 'bar'}]]
        )
        
        # Add all charts to subplots
        # ... (implementation details)
        
        fig.update_layout(
            title_text="YouTube Analytics Dashboard",
            showlegend=True,
            height=800,
            width=1200
        )
        
        return fig
    
    def export_visualizations(self, fig, filename):
        """
        Export visualizations in multiple formats
        """
        # HTML interactive
        fig.write_html(f"{filename}.html")
        
        # Static image
        fig.write_image(f"{filename}.png", width=1920, height=1080, scale=2)
        
        # SVG for presentations
        fig.write_image(f"{filename}.svg")
        
        print(f"✅ Exported visualizations to {filename}")
