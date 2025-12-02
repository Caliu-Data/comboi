"""
Ecommerce Bruin Transformation: Product Recommendation Engine

This transformation generates product recommendations using:
- Collaborative filtering patterns
- Product co-occurrence analysis
- User behavior similarity
- Category affinity scoring

This requires complex analysis better suited for Python than SQL.
"""

import pandas as pd
import numpy as np
from collections import defaultdict


def transform(con, inputs):
    """
    Generate product recommendations based on user behavior.

    Args:
        con: DuckDB connection
        inputs: Dict with 'user_sessions' pointing to parquet file

    Returns:
        DataFrame with product recommendations per user
    """
    # Load session data with events
    # For this example, we'll work with aggregated session data
    sessions_df = con.execute("SELECT * FROM user_sessions").df()

    # In a real implementation, you'd load detailed event data
    # For demonstration, we'll create synthetic product view patterns

    # Simulate product view data (in production, this would come from events table)
    np.random.seed(42)
    product_views = []

    for _, session in sessions_df.iterrows():
        num_products = min(session['products_viewed'], 10)
        for _ in range(num_products):
            product_views.append({
                'session_id': session['session_id'],
                'user_id': session['user_id'],
                'product_id': f"PROD_{np.random.randint(1, 100)}",
                'conversion_flag': session['conversion_flag']
            })

    product_df = pd.DataFrame(product_views)

    # Calculate user-product interaction matrix
    user_product_interactions = product_df.groupby(['user_id', 'product_id']).size().reset_index(name='interaction_count')

    # Calculate product co-occurrence (products viewed in same session)
    session_products = product_df.groupby('session_id')['product_id'].apply(list).reset_index()

    # Build co-occurrence matrix
    cooccurrence = defaultdict(lambda: defaultdict(int))

    for products in session_products['product_id']:
        for i, prod1 in enumerate(products):
            for prod2 in products[i+1:]:
                cooccurrence[prod1][prod2] += 1
                cooccurrence[prod2][prod1] += 1

    # Calculate product popularity
    product_popularity = product_df.groupby('product_id').agg({
        'session_id': 'count',
        'conversion_flag': 'sum'
    }).reset_index()
    product_popularity.columns = ['product_id', 'view_count', 'conversion_count']
    product_popularity['conversion_rate'] = (
        product_popularity['conversion_count'] / product_popularity['view_count']
    )

    # Generate recommendations for each user
    recommendations = []

    for user_id in sessions_df[sessions_df['user_id'].notna()]['user_id'].unique():
        # Get products this user has viewed
        user_products = product_df[product_df['user_id'] == user_id]['product_id'].unique()

        if len(user_products) == 0:
            continue

        # Find related products based on co-occurrence
        related_products = defaultdict(float)

        for viewed_product in user_products:
            if viewed_product in cooccurrence:
                for related_prod, count in cooccurrence[viewed_product].items():
                    if related_prod not in user_products:
                        # Score based on co-occurrence and popularity
                        popularity_score = product_popularity[
                            product_popularity['product_id'] == related_prod
                        ]['conversion_rate'].values

                        if len(popularity_score) > 0:
                            related_products[related_prod] += count * (1 + popularity_score[0])
                        else:
                            related_products[related_prod] += count

        # Get top 5 recommendations
        top_recommendations = sorted(
            related_products.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]

        for rank, (product_id, score) in enumerate(top_recommendations, 1):
            recommendations.append({
                'user_id': user_id,
                'recommended_product_id': product_id,
                'recommendation_rank': rank,
                'recommendation_score': round(score, 2),
                'recommendation_type': 'collaborative_filtering'
            })

    # Calculate user affinity scores
    user_sessions_agg = sessions_df.groupby('user_id').agg({
        'session_id': 'count',
        'conversion_flag': 'sum',
        'total_revenue': 'sum',
        'device_type': lambda x: x.mode()[0] if len(x) > 0 else 'desktop'
    }).reset_index()

    user_sessions_agg.columns = [
        'user_id', 'total_sessions', 'total_purchases',
        'lifetime_revenue', 'preferred_device'
    ]

    # Calculate engagement score
    user_sessions_agg['engagement_score'] = (
        np.log1p(user_sessions_agg['total_sessions']) * 10 +
        np.log1p(user_sessions_agg['total_purchases']) * 20 +
        np.log1p(user_sessions_agg['lifetime_revenue']) * 5
    )
    user_sessions_agg['engagement_score'] = user_sessions_agg['engagement_score'].clip(0, 100)

    # Merge recommendations with user metrics
    recommendations_df = pd.DataFrame(recommendations)

    if len(recommendations_df) > 0:
        recommendations_df = recommendations_df.merge(
            user_sessions_agg[['user_id', 'engagement_score', 'preferred_device']],
            on='user_id',
            how='left'
        )

        # Boost recommendations for high-engagement users
        recommendations_df['adjusted_score'] = (
            recommendations_df['recommendation_score'] *
            (1 + recommendations_df['engagement_score'] / 100)
        )

        # Re-rank within each user
        recommendations_df['final_rank'] = recommendations_df.groupby('user_id')['adjusted_score'].rank(
            ascending=False, method='first'
        ).astype(int)

        result = recommendations_df[[
            'user_id',
            'recommended_product_id',
            'final_rank',
            'adjusted_score',
            'recommendation_type',
            'engagement_score',
            'preferred_device'
        ]]

        result.columns = [
            'user_id',
            'product_id',
            'recommendation_rank',
            'recommendation_score',
            'recommendation_type',
            'user_engagement_score',
            'preferred_device'
        ]

        return result
    else:
        # Return empty dataframe with correct schema
        return pd.DataFrame(columns=[
            'user_id', 'product_id', 'recommendation_rank',
            'recommendation_score', 'recommendation_type',
            'user_engagement_score', 'preferred_device'
        ])
