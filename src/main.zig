const std = @import("std");
const pgzx = @import("pgzx");
const pg = pgzx.c;

const bufPrint = std.fmt.bufPrint;

comptime {
    pgzx.PG_MODULE_MAGIC();

    pgzx.PG_FUNCTION_V1("fdw_sequential_ints_handler", fdwSequentialIntsHandler);
}

pub const std_options = .{
    .log_level = .debug,
    .logFn = pgzx.elog.logFn,
};

pub export fn _PG_init() void {
    pgzx.elog.options.postgresLogFnLeven = pg.NOTICE;
}

fn getForeignRelSize(planner_info: [*c]pg.struct_PlannerInfo, rel_opt_info: [*c]pg.struct_RelOptInfo, oid: c_uint) callconv(.C) void {
    _ = planner_info;
    _ = oid;

    rel_opt_info.*.rows = 5;
}

fn getForeignPaths(root: [*c]pg.struct_PlannerInfo, baserel: [*c]pg.struct_RelOptInfo, foreigntableid: c_uint) callconv(.C) void {
    _ = foreigntableid;

    const startup_cost: pg.Cost = 0;
    const total_cost: pg.Cost = baserel.*.rows;

    const new_path: [*c]pg.struct_ForeignPath = pg.create_foreignscan_path(root, baserel, null, baserel.*.rows, startup_cost, total_cost, null, null, null, null);
    pg.add_path(baserel, @ptrCast(@alignCast(new_path))); // Somehow this casting worked, but I don't actually have a clue why...
}

fn getForeignPlan(planner_info: [*c]pg.struct_PlannerInfo, baserel: [*c]pg.struct_RelOptInfo, oid: c_uint, foreign_path: [*c]pg.struct_ForeignPath, tlist: [*c]pg.struct_List, scan_clauses: [*c]pg.struct_List, outer_plan: [*c]pg.struct_Plan) callconv(.C) [*c]pg.struct_ForeignScan {
    _ = planner_info;
    _ = oid;
    _ = foreign_path;

    const actual_clauses = pg.extract_actual_clauses(scan_clauses, false);
    return pg.make_foreignscan(tlist, actual_clauses, baserel.*.relid, null, null, null, null, outer_plan);
}

fn reScanForeignScan(node: [*c]pg.ForeignScanState) callconv(.C) void {
    const my_scan_state: *MyScanState = @as(*MyScanState, @ptrCast(node.*.fdw_state));
    my_scan_state.current_row = 0;
}

const MyScanState = struct { current_row: i8 };

fn beginForeignScan(node: [*c]pg.ForeignScanState, eflags: i32) callconv(.C) void {
    _ = eflags;
    pgzx.elog.Info(@src(), "beginForeignScan called", .{});

    var memctx = pgzx.mem.createAllocSetContext("zig_context", .{ .parent = pg.CurrentMemoryContext }) catch |err| {
        pgzx.elog.Warning(@src(), "failed to create memctx: {any}", .{err});
        @panic("failed to create memctx");
    };
    const allocator = memctx.allocator();

    const mss = allocator.create(MyScanState) catch |err| {
        pgzx.elog.Warning(@src(), "failed to allocate MyScanState {any}", .{err});
        @panic("failed to allocation MyScanState");
    };
    mss.current_row = 0;
    node.*.fdw_state = mss;
}

fn iterateForeignScan(node: [*c]pg.ForeignScanState) callconv(.C) [*c]pg.TupleTableSlot {
    const slot = node.*.ss.ss_ScanTupleSlot;
    _ = pg.ExecClearTuple(slot);

    const my_scan_state: *MyScanState = @as(*MyScanState, @ptrCast(node.*.fdw_state));

    if (my_scan_state.current_row < 100) {
        const values = [_]pg.Datum{pg.Int32GetDatum(my_scan_state.current_row + 1)};
        const values_ptr = @constCast(values[0..].ptr);

        const nulls = [_]bool{false};
        const nulls_ptr = @constCast(nulls[0..].ptr);

        my_scan_state.current_row += 1;

        _ = pg.ExecStoreHeapTuple(pg.heap_form_tuple(slot.*.tts_tupleDescriptor, values_ptr, nulls_ptr), slot, false);
    }

    return slot;
}

fn endForeignScan(scan_state: [*c]pg.ForeignScanState) callconv(.C) void {
    _ = scan_state;
}

fn fdwSequentialIntsHandler() pg.Datum {
    const routines: *pg.FdwRoutine = makeFdwRoutineNode();
    routines.*.GetForeignRelSize = getForeignRelSize;
    routines.*.GetForeignPaths = getForeignPaths;
    routines.*.GetForeignPlan = getForeignPlan;
    routines.*.BeginForeignScan = beginForeignScan;
    routines.*.IterateForeignScan = iterateForeignScan;
    routines.*.ReScanForeignScan = reScanForeignScan;
    routines.*.EndForeignScan = endForeignScan;

    return pg.PointerGetDatum(routines);
}

pub fn makeFdwRoutineNode() *pg.FdwRoutine {
    return makeNode(pg.FdwRoutine, pg.T_FdwRoutine);
}

pub fn makeNode(comptime T: type, comptime tag: pg.NodeTag) *T {
    const node: *pg.Node = @ptrCast(@alignCast(pg.palloc0fast(@sizeOf(T))));
    node.*.type = tag;
    return @ptrCast(@alignCast(node));
}
