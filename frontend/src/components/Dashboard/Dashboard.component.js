import React, { useEffect, useRef } from 'react';
import Sentiment from '../Sentiment/Sentiment.component';
import Chart from '../Chart/Chart.component';

import $ from 'jquery';
import 'jquery-ui/ui/widgets/resizable';
// CSS
import './Dashboard.component.css';
import StockList from '../Stocks/StockList/StockList.component';


const Dashboard = () => {
    const containerRef = useRef(null);
    const leftRef = useRef(null);
    const rightRef = useRef(null);
  
    useEffect(() => {
      const $left = $(leftRef.current);
      const $right = $(rightRef.current);
      const $container = $(containerRef.current);
  
      const totalWidth = $container.width();
  
      $left.resizable({
        handles: 'e',
        minWidth: 100,
        maxWidth: totalWidth - 100,
        resize: function (event, ui) {
          const newLeftWidth = ui.size.width;
          const newRightWidth = totalWidth - newLeftWidth;
          $right.width(newRightWidth);
        }
      });
  
      return () => {
        $left.resizable('destroy');
      };
    }, []);
    return (
        <div className='dashboard-container' ref={containerRef}>
            <div className='dashboard-left' ref={leftRef}>
                <Chart />
            </div>
            <div className='dashboard-right' ref={rightRef}>
                <div className='dashboard-right-top'>
                    <StockList />
                </div>
                <div className='dashboard-right-bottom'>
                    <Sentiment />
                </div>
            </div>
        </div>
    );
};

export default Dashboard;