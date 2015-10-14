var margin = {top: 20, right: 20, bottom: 20, left: 100},
		width = 840 - margin.right - margin.left,
		height = 300 - margin.top - margin.bottom;

var minNPV, maxNPV;
var minCashflow, maxCashflow;

var i = 0,
		duration = 750,
		root;

var currentDepth = 0;

var tree = d3.layout.tree()
		.size([height, width]);

var diagonal = d3.svg.diagonal()
		.projection(function(d) { return [3*d.y, d.x]; });

var svg = d3.select("#tree-container").append("svg")
		.attr("width", width + margin.right + margin.left)
		.attr("height", height + margin.top + margin.bottom)
	.append("g")
		.attr("transform", "translate(" + 3*margin.left + "," + margin.top + ")");

var backward = svg.append("text")
	.attr("class", "button")
	.attr("transform", "translate(0, 0)")
	.on("click", function() {goBackward();})
	.text("<");

var forward = svg.append("text")
	.attr("class", "button")
	.attr("transform", "translate(20, 0)")
	.on("click", function() {goForward();})
	.text(">");


//d3.csv("tree_data.csv", function(error, unparsedData) {
//	console.log("hello!");
//	console.log(unparsedData);
	//data = d3.csv.parseRows(unparsedData);
//	console.log("hello");
	root =  parseData(lindaData);
	console.log("hello");
	root.x0 = height / 2;
	root.y0 = 0;

	function collapse(d) {
		if (d.children) {
			d._children = d.children;
			d._children.forEach(collapse);
			d.children = null;
		}
	}

	root.children.forEach(collapse);
	console.log("root");
	console.log(root);
	update(root);
//});

d3.select(self.frameElement).style("height", "800px");

var nodes;

function update(source) {

	// Compute the new tree layout.
	nodes = tree.nodes(root).reverse();
	console.log("nodes: " + nodes);
	var links = tree.links(nodes);

	// Normalize for fixed-depth.
	nodes.forEach(function(d) { d.y = d.depth * 50; });

	// Update the nodes…
	var node = svg.selectAll("g.node")
			.data(nodes, function(d) { return d.id || (d.id = ++i); });

	// Enter any new nodes at the parent's previous position.
	var nodeEnter = node.enter().append("g")
			.attr("class", "node")
			.attr("transform", function(d) { return "translate(" + 3*source.y0 + "," + source.x0 + ")"; })
			.on("click", expand);

	nodeEnter.append("circle")
			.attr("r", 1e-6)
			.style("fill", function(d) { return d._children ? "lightsteelblue" : "#fff"; });

	nodeEnter.append("text")
			.attr("x", function(d) { return d.children || d._children ? -10 : 10; })
			.attr("dy", ".25em")
			.attr("text-anchor", function(d) { return d.children || d._children ? "end" : "start"; })
			.text(function(d) { return d.npv; })
			.style("fill-opacity", 1e-6);

	// Transition nodes to their new position.
	var nodeUpdate = node.transition()
			.duration(duration)
			.attr("transform", function(d) { return "translate(" + 3*d.y + "," + d.x + ")"; });

	nodeUpdate.select("circle")
			.attr("r", 6.5)
			 .style("fill", function(d) { return d._children ? "lightsteelblue" : "#fff"; });
			/*.attr("r", function(d) {
				return (d.cashflow - minCashflow) /  (maxCashflow - minCashflow) * 10 + 5;
			})
			.style("fill", function(d) {
				if (d._children)
					return "lightsteelblue";
				else {	
					if (d.npv < 0) {
						var map_value = (d.npv - minNPV) /  (-1 * minNPV) * 255;
						return d3.rgb(255, map_value, map_value);
					}
					else {
						var map_value = (d.npv) / (maxNPV) * 255;
						return d3.rgb(255 - map_value, 255, 255 - map_value);
					}
				}
			})*/;

	nodeUpdate.select("text")
			.style("fill-opacity", 1);

	// Transition exiting nodes to the parent's new position.
	var nodeExit = node.exit().transition()
			.duration(duration)
			.attr("transform", function(d) { return "translate(" + 3*source.y + "," + source.x + ")"; })
			.remove();

	nodeExit.select("circle")
			.attr("r", 1e-6);

	nodeExit.select("text")
			.style("fill-opacity", 1e-6);

	// Update the links…
	var link = svg.selectAll("path.link")
			.data(links, function(d) { return d.target.id; });

	// Enter any new links at the parent's previous position.
	link.enter().insert("path", "g")
			.attr("class", "link")
			.attr("d", function(d) {
				var o = {x: source.x0, y: source.y0};
				return diagonal({source: o, target: o});
			});

	// Transition links to their new position.
	link.transition()
			.duration(duration)
			.attr("d", diagonal);

	// Transition exiting nodes to the parent's new position.
	link.exit().transition()
			.duration(duration)
			.attr("d", function(d) {
				var o = {x: source.x, y: source.y};
				return diagonal({source: o, target: o});
			})
			.remove();

	// Stash the old positions for transition.
	nodes.forEach(function(d) {
		d.x0 = d.x;
		d.y0 = d.y;
	});
}

// Toggle children on click.
function expand(d) {
	if (d.children) {
		d._children = d.children;
		d.children = null;
	} else {
		d.children = d._children;
		d._children = null;
	}
	update(d);
}

function goForward() {
	nodes.forEach(function(d) {
		if (d.depth == currentDepth)
			expand(d);
	});
	currentDepth++;
}

function goBackward() {
	nodes.forEach(function(d) {
		if (d.depth == currentDepth - 1)
			expand(d);
	});
	currentDepth--;
}

function TreeNode(index, parent_index, npv, cashflow) {
	// Private properties

	// Public properties
	this.children = [];
	this.index = index;
	this.parent_index = parent_index;
	this.npv = npv;
	this.cashflow = cashflow;

};

function parseData(data) {
	var treeNodes = [];
	for (var i = 0; i < data.length; i++) {
		var treeNode = new TreeNode(data[i][0], data[i][1], data[i][2], parseInt(data[i][3]));

		for (var j = 0; j < treeNodes.length; j++) {
			if (treeNodes[j].index == treeNode.parent_index){ //.trim()) {
				treeNodes[j].children.push(treeNode);
				break;
			}
		}
		treeNodes.push(treeNode);
	}

	for (var j = 0; j < treeNodes.length; j++) {
		if (treeNodes[j].children.length == 0) treeNodes[j].children = null;
	}

	//calculateBoundingValues(treeNodes);

	return treeNodes[0];
}

function calculateBoundingValues(treeNodes) {
	minNPV = Number.MAX_VALUE;
	maxNPV = Number.MIN_VALUE;
	minCashflow = Number.MAX_VALUE;
	maxCashflow = Number.MIN_VALUE;

	for (var j = 0; j < treeNodes.length; j++) {
		if (treeNodes[j].npv < minNPV)
			minNPV = treeNodes[j].npv; 

		if (treeNodes[j].npv > maxNPV)
			maxNPV = treeNodes[j].npv;

		if (treeNodes[j].cashflow < minCashflow)
			minCashflow = treeNodes[j].cashflow; 

		if (treeNodes[j].cashflow > maxCashflow)
			maxCashflow = treeNodes[j].cashflow;
	}
}