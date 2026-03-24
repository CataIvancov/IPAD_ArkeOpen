import React from "react";

const GEOLOGICAL_PERIODS = [
  { name: "Early Pleistocene", start: -2500000, end: -498050, color: "#f39306" },
  { name: "Middle Pleistocene", start: -498050, end: -127050, color: "#f84c48" },
  { name: "Late Pleistocene", start: -127050, end: -11700, color: "#c96fd6" },
  { name: "Early Holocene", start: -11700, end: -8200, color: "#6b8e23" },
  { name: "Mid Holocene", start: -8200, end: -4200, color: "#8fbc8f" },
  { name: "Late Holocene", start: -4200, end: 1950, color: "#c8d8b8" },
];

function yearToX(year, minX, maxX, width) {
  const safeMaxX = Math.max(Math.abs(maxX), 1);
  const logMin = Math.log(Math.abs(minX));
  const logMax = Math.log(safeMaxX);
  const logYear = Math.log(Math.max(Math.abs(year), 1));
  const normalized = (logYear - logMin) / (logMax - logMin);

  return width * (1 - normalized);
}

export function GeologicalTimeline() {
  const svgWidth = 900;
  const svgHeight = 250;
  const timelineY = 145;
  const geologicalY = 70;
  const barHeight = 40;
  const minX = -2580000;
  const maxX = 1950;
  const markers = [
    { year: -2580000, label: "~2.58 mya" },
    { year: -500000, label: "~500 kya" },
    { year: -127050, label: "~127 kya" },
    { year: -11700, label: "~11.7 kya" },
    { year: -8200, label: "~8.2 kya" },
    { year: -4200, label: "~4.2 kya" },
    { year: 1950, label: "1950" },
  ];

  return (
    <div className="geological-timeline">
      <h2>GEOLOGICAL TIMELINE</h2>
      <svg viewBox={`0 0 ${svgWidth} ${svgHeight}`} className="timeline-svg">
        <text
          x="20"
          y={geologicalY - 8}
          className="bar-label"
          fill="#333"
          fontSize="12"
          fontWeight="bold"
        >
          PLEISTOCENE & HOLOCENE
        </text>

        {GEOLOGICAL_PERIODS.map((period) => {
          const startX = yearToX(period.start, minX, maxX, svgWidth);
          const endX = yearToX(period.end, minX, maxX, svgWidth);
          const width = Math.max(endX - startX, 2);

          return (
            <g key={period.name}>
              <rect
                x={startX}
                y={geologicalY}
                width={width}
                height={barHeight}
                fill={period.color}
                opacity="0.85"
                rx="4"
              />
              <text
                x={(startX + endX) / 2}
                y={geologicalY + 24}
                textAnchor="middle"
                className="bar-label"
                fill={period.name.includes("Holocene") ? "#000" : "#fff"}
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
          markerEnd="url(#geo-arrowhead)"
        />

        <defs>
          <marker
            id="geo-arrowhead"
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
          <span className="legend-heading">Geological Subperiods</span>
          <div className="legend-grid">
            <div className="legend-item">
              <span className="legend-color" style={{ backgroundColor: "#f39306" }}></span>
              <span>Early Pleistocene (2.5 mya - 498 kya)</span>
            </div>
            <div className="legend-item">
              <span className="legend-color" style={{ backgroundColor: "#f84c48" }}></span>
              <span>Middle Pleistocene (498 kya - 127 kya)</span>
            </div>
            <div className="legend-item">
              <span className="legend-color" style={{ backgroundColor: "#c96fd6" }}></span>
              <span>Late Pleistocene (127 kya - 11.7 kya)</span>
            </div>
            <div className="legend-item">
              <span className="legend-color" style={{ backgroundColor: "#6b8e23" }}></span>
              <span>Early Holocene (11.7 kya - 8.2 kya)</span>
            </div>
            <div className="legend-item">
              <span className="legend-color" style={{ backgroundColor: "#8fbc8f" }}></span>
              <span>Mid Holocene (8.2 kya - 4.2 kya)</span>
            </div>
            <div className="legend-item">
              <span className="legend-color" style={{ backgroundColor: "#c8d8b8" }}></span>
              <span>Late Holocene (4.2 kya - 1950)</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
