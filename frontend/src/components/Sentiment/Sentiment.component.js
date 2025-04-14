/*
File: Finance Project/frontend/src/components/AnalysisPanel/AnalysisPanel.component.js
Description: Component to display the 5+ analysis outcomes for the selected stock.
             Fetches data from the backend /analysis/{symbol} endpoint.
             (Formerly Sentiment.component.js)
*/
import React, { useState, useEffect } from 'react';
// Renamed CSS import to match component name (optional)
import './AnalysisPanel.component.css';

// Renamed component function
const AnalysisPanel = ({ selectedStock }) => {
    // Single state to hold the fetched analysis object
    const [analysisData, setAnalysisData] = useState(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState(null);

    useEffect(() => {
        const fetchAnalysis = async (stock) => {
            if (!stock || !stock.symbol) {
                // Clear state if stock is invalid or deselected
                setAnalysisData(null);
                setIsLoading(false);
                setError(null);
                return;
            }

            setIsLoading(true);
            setError(null);
            setAnalysisData(null); // Clear previous data before fetching new

            const backendUrl = process.env.REACT_APP_API_URL || 'http://localhost:8000';
            // --- UPDATED: Use the correct analysis endpoint ---
            const analysisUrl = `${backendUrl}/api/v1/stocks/analysis/${stock.symbol}`;

            console.log(`Fetching analysis for ${stock.symbol} from ${analysisUrl}`);

            try {
                const response = await fetch(analysisUrl);
                if (!response.ok) {
                    let errorDetail = `Failed to fetch analysis (${response.status})`;
                    try {
                        const errData = await response.json();
                        // Use message from backend if available
                        errorDetail = errData.message || errData.detail || errorDetail;
                    } catch (e) { /* ignore if response is not JSON */ }
                    throw new Error(errorDetail);
                }
                const result = await response.json();

                // Check the status and presence of analysis data in the response
                if (result && result.status === 'found' && result.analysis) {
                    setAnalysisData(result.analysis); // Set the fetched analysis object
                    console.log(`Analysis data received for ${stock.symbol}`);
                } else if (result && result.status === 'not_found') {
                    setAnalysisData(null); // Explicitly set to null if not found
                    console.log(`No analysis data found yet for ${stock.symbol}`);
                    // Optionally set a specific message instead of error
                    // setError("Analysis data not yet available for this stock.");
                } else {
                    // Handle unexpected response format
                    throw new Error(result.message || "Invalid analysis response format received");
                }

            } catch (err) {
                console.error(`Error fetching analysis data for ${stock.symbol}:`, err);
                setError(err.message || "Failed to load analysis data.");
                setAnalysisData(null); // Clear data on error
            } finally {
                setIsLoading(false);
            }
        };

        fetchAnalysis(selectedStock); // Call fetchAnalysis when selectedStock changes

    }, [selectedStock]); // Re-run effect when selectedStock changes

    // Helper function to display analysis values, handling null/undefined
    const displayValue = (value, precision = undefined) => {
        if (value === null || value === undefined) {
            return 'N/A';
        }
        if (typeof value === 'boolean') {
            return value ? 'Yes' : 'No';
        }
        if (typeof value === 'number' && precision !== undefined) {
            return value.toFixed(precision);
        }
        return String(value);
    };

    // --- Render Component UI ---
    return (
        // Consider renaming class if CSS file is renamed
        <div className='analysis-panel-container'>
            <h4>Analysis {selectedStock ? `for ${selectedStock.symbol}` : ''}</h4>

            {isLoading && <p className="loading-message">Loading analysis...</p>}
            {error && <p className="error-message">Error: {error}</p>}
            {!selectedStock && !isLoading && !error && (
                <p className="placeholder-message">Select a stock to view analysis.</p>
            )}

            {/* Display analysis content only if a stock is selected and not loading/error */}
            {selectedStock && !isLoading && !error && (
                <div className="analysis-content">
                    {analysisData ? (
                        <>
                            {/* --- UPDATED: Render the 5+ specific outcomes --- */}
                            <div className="analysis-item">
                                <strong>Sentiment:</strong>
                                <span>{displayValue(analysisData.sentiment_label)} {analysisData.sentiment_score !== null ? `(${displayValue(analysisData.sentiment_score, 2)})` : ''}</span>
                            </div>
                            <div className="analysis-item">
                                <strong>Short-term:</strong>
                                <span>{displayValue(analysisData.short_term_label)}</span>
                            </div>
                             <div className="analysis-item">
                                <strong>Volatility:</strong>
                                <span>{displayValue(analysisData.volatility_label)} {analysisData.volatility_score !== null ? `(${displayValue(analysisData.volatility_score, 3)})` : ''}</span>
                            </div>
                             <div className="analysis-item">
                                <strong>Risk:</strong>
                                <span>{displayValue(analysisData.risk_label)} {analysisData.risk_score !== null ? `(${displayValue(analysisData.risk_score, 3)})` : ''}</span>
                            </div>
                            <div className="analysis-item">
                                <strong>Liquidity Crunch:</strong>
                                <span>{displayValue(analysisData.liquidity_crunch)}</span>
                            </div>
                             <div className="analysis-item">
                                <strong>Mood Index:</strong>
                                <span>{displayValue(analysisData.mood_index_label)} {analysisData.mood_index_score !== null ? `(${displayValue(analysisData.mood_index_score, 2)})` : ''}</span>
                            </div>

                            {/* Optionally display the timestamp */}
                            {analysisData.time && (
                                <p className="timestamp"><small>Last Updated: {new Date(analysisData.time).toLocaleString()}</small></p>
                            )}
                        </>
                    ) : (
                        // Display message if analysis data is specifically not found (vs. error/loading)
                         <p className="placeholder-message">Analysis data not yet available for this stock.</p>
                    )}
                </div>
            )}
        </div>
    );
};

// Rename export if component name changed
export default AnalysisPanel;

