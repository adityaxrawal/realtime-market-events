/*
File: Finance Project/frontend/src/components/Sentiment/Sentiment.component.js
Description: Component to display sentiment analysis and market regime information
             for the currently selected stock. Fetches data from placeholder backend endpoints.
*/
import React, { useState, useEffect } from 'react';
import './Sentiment.component.css'; // Import the associated CSS

const Sentiment = ({ selectedStock }) => {
    // State to hold fetched sentiment data
    const [sentimentData, setSentimentData] = useState(null);
    // State to hold fetched market regime data
    const [regimeData, setRegimeData] = useState(null);
    // State for loading indicator
    const [isLoading, setIsLoading] = useState(false);
    // State for displaying errors
    const [error, setError] = useState(null);

    // Effect to fetch data when the selected stock changes
    useEffect(() => {
        // Function to fetch both sentiment and regime data
        const fetchSentimentAndRegime = async (stock) => {
            // Guard clause: Do nothing if stock or symbol is missing
            if (!stock || !stock.symbol) return;

            // Set loading state and clear previous data/errors
            setIsLoading(true);
            setError(null);
            setSentimentData(null);
            setRegimeData(null);

            // Retrieve backend URL from environment variables or use default
            const backendUrl = process.env.REACT_APP_API_URL || 'http://localhost:8000';
            // Construct API URLs (these are placeholders as per roadmap)
            const sentimentUrl = `${backendUrl}/api/v1/stocks/sentiment/${stock.symbol}`;
            const regimeUrl = `${backendUrl}/api/v1/stocks/regime/${stock.symbol}`;

            console.log(`Attempting to fetch analysis for ${stock.symbol}...`);

            try {
                // --- Fetch Sentiment Data (Placeholder Logic) ---
                // In a real implementation, you would fetch from sentimentUrl.
                // Here, we simulate a delay and set a placeholder message indicating
                // that the backend endpoint is not yet fully implemented.
                console.log(`Fetching sentiment from (placeholder): ${sentimentUrl}`);
                // Example of actual fetch (commented out):
                // const sentimentResponse = await fetch(sentimentUrl);
                // if (!sentimentResponse.ok) throw new Error(`Failed to fetch sentiment (${sentimentResponse.status})`);
                // const sentData = await sentimentResponse.json();
                // setSentimentData(sentData); // e.g., { score: 0.6, label: 'Positive' }

                // Simulate network delay for placeholder
                await new Promise(resolve => setTimeout(resolve, 300));
                // Set placeholder data indicating backend status
                setSentimentData({ status: 'not_implemented', message: 'Sentiment endpoint pending.' });

                // --- Fetch Regime Data (Placeholder Logic) ---
                // Similar placeholder logic for the market regime endpoint.
                console.log(`Fetching regime from (placeholder): ${regimeUrl}`);
                // Example of actual fetch (commented out):
                // const regimeResponse = await fetch(regimeUrl);
                // if (!regimeResponse.ok) throw new Error(`Failed to fetch regime (${regimeResponse.status})`);
                // const regData = await regimeResponse.json();
                // setRegimeData(regData); // e.g., { type: 'Trend', label: 'Trending Up' }

                // Simulate network delay for placeholder
                await new Promise(resolve => setTimeout(resolve, 300));
                // Set placeholder data indicating backend status
                setRegimeData({ status: 'not_implemented', message: 'Regime endpoint pending.' });

            } catch (err) {
                // Handle errors during fetch operations
                console.error(`Error fetching analysis data for ${stock.symbol}:`, err);
                setError(err.message || "Failed to load analysis data.");
            } finally {
                // Ensure loading state is turned off regardless of success or failure
                setIsLoading(false);
            }
        };

        // Trigger fetch only if a stock is selected
        if (selectedStock) {
            fetchSentimentAndRegime(selectedStock);
        } else {
            // Clear all data and states if no stock is selected
            setSentimentData(null);
            setRegimeData(null);
            setIsLoading(false);
            setError(null);
        }
    }, [selectedStock]); // Dependency: Re-run effect when selectedStock changes

    // --- Render Component UI ---
    return (
        <div className='sentiment-container'>
            {/* Display title, dynamically including the selected stock symbol */}
            <h4>Analysis {selectedStock ? `for ${selectedStock.symbol}` : ''}</h4>

            {/* Display loading indicator */}
            {isLoading && <p className="loading-message">Loading analysis...</p>}

            {/* Display error message */}
            {error && <p className="error-message">Error: {error}</p>}

            {/* Display placeholder message when no stock is selected */}
            {!selectedStock && !isLoading && !error && (
                <p className="placeholder-message">Select a stock to view sentiment and regime analysis.</p>
            )}

            {/* Display analysis content only if a stock is selected and not loading/error */}
            {selectedStock && !isLoading && !error && (
                <div className="analysis-content">
                    {/* Sentiment Section */}
                    <div className="sentiment-section">
                        <h5>Sentiment</h5>
                        {sentimentData ? (
                            sentimentData.status === 'not_implemented' ? (
                                <p className="placeholder-message">{sentimentData.message}</p>
                            ) : (
                                /* Placeholder for actual sentiment data rendering */
                                <p>Score: {sentimentData.score ?? 'N/A'}, Label: {sentimentData.label ?? 'N/A'}</p>
                            )
                        ) : (
                            <p className="placeholder-message">No sentiment data available.</p>
                        )}
                    </div>

                    {/* Market Regime Section */}
                    <div className="regime-section">
                        <h5>Market Regime</h5>
                        {regimeData ? (
                            regimeData.status === 'not_implemented' ? (
                                <p className="placeholder-message">{regimeData.message}</p>
                            ) : (
                                /* Placeholder for actual regime data rendering */
                                <p>Type: {regimeData.type ?? 'N/A'}, Label: {regimeData.label ?? 'N/A'}</p>
                            )
                        ) : (
                            <p className="placeholder-message">No market regime data available.</p>
                        )}
                    </div>
                </div>
            )}
        </div>
    );
};

export default Sentiment;
