import React from "react";

const ARCHAEOLOGICAL_PERIODS = [
  { name: "Palaeolithic", start: -2000000, end: -4000, color: "#5c8bc4" },
  { name: "Neolithic", start: -4000, end: -350, color: "#f39306" },
  { name: "Early Metal Age", start: -550, end: 400, color: "#f84c48" },
  { name: "Protohistory", start: 400, end: 1500, color: "#c96fd6" },
];

function yearToX(year, minX, maxX, width) {
  const span = maxX - minX;
  return ((year - minX) / span) * width;
}

export function ArchaeologicalTimeline() {
  const svgWidth = 900;
  const svgHeight = 250;
  const timelineY = 145;
  const archaeologyY = 70;
  const barHeight = 40;
  const minX = -2000000;
  const maxX = 1500;
  const markers = [
    { year: -2000000, label: "~2.0 mya" },
    { year: -4000, label: "4000 BCE" },
    { year: -350, label: "350 BCE" },
    { year: 400, label: "400 CE" },
    { year: 1500, label: "1500 CE" },
  ];

  return (
    <div className="geological-timeline">
      <h2>ARCHAEOLOGICAL CHRONOLOGY</h2>
      <svg viewBox={`0 0 ${svgWidth} ${svgHeight}`} className="timeline-svg">
        <text
          x="20"
          y={archaeologyY - 8}
          className="bar-label"
          fill="#333"
          fontSize="12"
          fontWeight="bold"
        >
          ARCHAEOLOGICAL PERIODS
        </text>

        {ARCHAEOLOGICAL_PERIODS.map((period) => {
          const startX = yearToX(period.start, minX, maxX, svgWidth);
          const endX = yearToX(period.end, minX, maxX, svgWidth);
          const width = Math.max(endX - startX, 2);

          return (
            <g key={period.name}>
              <rect
                x={startX}
                y={archaeologyY}
                width={width}
                height={barHeight}
                fill={period.color}
                opacity="0.85"
                rx="4"
              />
              <text
                x={(startX + endX) / 2}
                y={archaeologyY + 24}
                textAnchor="middle"
                className="bar-label"
                fill="#fff"
                fontSize="10"
                fontWeight="bold"
              >
                {period.name}
              </text>
            </g>
          );
        })}

        <line
          x1="20"
          y1={timelineY}
          x2={svgWidth - 20}
          y2={timelineY}
          stroke="#666"
          strokeWidth="2"
          markerEnd="url(#arch-arrowhead)"
        />

        <defs>
          <marker
            id="arch-arrowhead"
            markerWidth="10"
            markerHeight="7"
            refX="9"
            refY="3.5"
            orient="auto"
          >
            <polygon points="0 0, 10 3.5, 0 7" fill="#666" />
          </marker>
        </defs>

        {markers.map((marker) => {
          const x = yearToX(marker.year, minX, maxX, svgWidth);

          return (
            <g key={marker.label}>
              <line
                x1={x}
                y1={timelineY - 10}
                x2={x}
                y2={timelineY + 10}
                stroke="#666"
                strokeWidth="1"
                strokeDasharray="2,2"
              />
              <text
                x={x}
                y={timelineY + 24}
                textAnchor="middle"
                className="timeline-label"
                fill="#333"
                fontSize="10"
              >
                {marker.label}
              </text>
            </g>
          );
        })}
      </svg>

      <div className="timeline-legend">
        <div className="legend-section">
          <span className="legend-heading">Archaeological Periods</span>
          <div className="legend-grid">
            <div className="legend-item">
              <span className="legend-color" style={{ backgroundColor: "#5c8bc4" }}></span>
              <span>Palaeolithic (2.0 mya - 4000 BCE)</span>
            </div>
            <div className="legend-item">
              <span className="legend-color" style={{ backgroundColor: "#f39306" }}></span>
              <span>Neolithic (4000 BCE - 350 BCE)</span>
            </div>
            <div className="legend-item">
              <span className="legend-color" style={{ backgroundColor: "#f84c48" }}></span>
              <span>Early Metal Age (550 BCE - 400 CE)</span>
            </div>
            <div className="legend-item">
              <span className="legend-color" style={{ backgroundColor: "#c96fd6" }}></span>
              <span>Protohistory (400 CE - 1500 CE)</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
