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

// var prev_ExecutorStart_hook: pg.ExecutorStart_hook_type = null;

pub export fn _PG_init() void {
    pgzx.elog.options.postgresLogFnLeven = pg.NOTICE;

    // prev_ExecutorStart_hook = pg.ExecutorStart_hook;
    // pg.ExecutorStart_hook = executorStartHook;
}

fn logFunctionCall(src: std.builtin.SourceLocation) void {
    pgzx.elog.Info(src, "{s} called", .{src.fn_name});
}

// fn myExecutorCallback(ctx: pg.MemoryContext) void {
//     _ = ctx;
//
//     pgzx.elog.Info(@src(), "registerAllocResetCallback called for executor memory context", .{});
// }

// var processmemoryctx = pgzx.mem.createAllocSetContext("zig_context", .{ .parent = pg.CurrentMemoryContext }) catch |err| {
//     pgzx.elog.Warning("failed to create memctx: {any}", .{err});
//     @panic("failed to create memctx");
// };
// var my_chunk_of_memory = processmemoryctx.allocator().alloc(i32, 10) catch |err| {
//     pgzx.elog.Warning("failed to allocate my_chunk_of_memory {any}", .{err});
//     @panic("failed to allocate my_chunk_of_memory ");
// };

// fn executorStartHook(queryDesc: [*c]pg.struct_QueryDesc, eflags: i32) callconv(.C) void {
//     pgzx.elog.Info(@src(), "executorStartHook called", .{});
//
//     if (prev_ExecutorStart_hook) |hook| {
//         hook(queryDesc, eflags);
//     } else {
//         // we still need to call the standard hook
//         pg.standard_ExecutorStart(queryDesc, eflags);
//     }
//
//     var memctx = pgzx.mem.createAllocSetContext("zig_context", .{ .parent = queryDesc.*.estate.*.es_query_cxt }) catch |err| {
//         pgzx.elog.Warning(@src(), "failed to create memctx: {any}", .{err});
//         @panic("failed to create memctx");
//     };
//
//     const mqs = memctx.allocator().create(MyQueryState) catch |err| {
//         pgzx.elog.Warning(@src(), "failed to allocate MyQueryState {any}", .{err});
//         @panic("failed to allocation MyQueryState");
//     };
//     mqs.my_fantastic_data = 78;
//
//     memctx.registerAllocResetCallback(
//         queryDesc.*.estate.*.es_query_cxt,
//         myExecutorCallback,
//     ) catch |err| {
//         pgzx.elog.Warning(@src(), "failed to register alloc reset callback: {any}", .{err});
//     };
// }

const MAX_ROWS = 10_000;

fn getForeignRelSize(root: [*c]pg.struct_PlannerInfo, baserel: [*c]pg.struct_RelOptInfo, foreigntableid: c_uint) callconv(.C) void {
    logFunctionCall(@src());

    _ = root;
    _ = foreigntableid;

    baserel.*.rows = MAX_ROWS;
}

fn getForeignPaths(root: [*c]pg.struct_PlannerInfo, baserel: [*c]pg.struct_RelOptInfo, foreigntableid: c_uint) callconv(.C) void {
    logFunctionCall(@src());

    _ = foreigntableid;

    const startup_cost: pg.Cost = 0;
    const total_cost: pg.Cost = baserel.*.rows;

    const new_path: [*c]pg.struct_ForeignPath = pg.create_foreignscan_path(root, baserel, null, baserel.*.rows, startup_cost, total_cost, null, null, null, null);
    pg.add_path(baserel, @ptrCast(@alignCast(new_path))); // Somehow this casting worked, but I don't actually have a clue why...
}

// fn getForeignJoinPaths(root: [*c]pg.struct_PlannerInfo, joinrel: [*c]pg.struct_RelOptInfo, outerrel: [*c]pg.struct_RelOptInfo, innerrel: [*c]pg.struct_RelOptInfo, jointype: pg.enum_JoinType, extra: [*c]pg.struct_JoinPathExtraData) callconv(.C) void {
//     logFunctionCall(@src());
//
//     pgzx.elog.Info(@src(), "root = {any}", .{root});
//     pgzx.elog.Info(@src(), "join type = {d}", .{jointype});
//
//     pgzx.elog.Info(@src(), "join rel = {any}", .{joinrel.*});
//
//     pgzx.elog.Info(@src(), "outer rel = {any}", .{outerrel.*});
//     pgzx.elog.Info(@src(), "inner rel = {any}", .{innerrel.*});
//     pgzx.elog.Info(@src(), "extra data = {any}", .{extra.*});
//
//     const joinpath = pg.create_foreign_join_path(root, joinrel, joinrel.*.reltarget, MAX_ROWS, 1.0, 1.0, null, null, null, null);
//
//     pg.add_path(joinrel, @ptrCast(@alignCast(joinpath))); // Somehow this casting worked, but I don't actually have a clue why...
// }

fn getForeignPlan(root: [*c]pg.struct_PlannerInfo, baserel: [*c]pg.struct_RelOptInfo, foreigntableid: c_uint, best_path: [*c]pg.struct_ForeignPath, tlist: [*c]pg.struct_List, scan_clauses: [*c]pg.struct_List, outer_plan: [*c]pg.struct_Plan) callconv(.C) [*c]pg.struct_ForeignScan {
    logFunctionCall(@src());

    _ = root;
    _ = foreigntableid;
    _ = best_path;

    const actual_clauses = pg.extract_actual_clauses(scan_clauses, false);
    return pg.make_foreignscan(tlist, actual_clauses, baserel.*.relid, null, null, null, null, outer_plan);
}

fn reScanForeignScan(node: [*c]pg.ForeignScanState) callconv(.C) void {
    logFunctionCall(@src());

    const my_scan_state: *MyScanState = @ptrCast(@alignCast(node.*.fdw_state));
    my_scan_state.current_row = 0;
}

const MyScanState = struct { current_row: i32 };

var scan_attempts: usize = 0;

var my_little_array: [MAX_ROWS]i32 = std.mem.zeroes([MAX_ROWS]i32);

fn beginForeignScan(node: [*c]pg.ForeignScanState, eflags: i32) callconv(.C) void {
    logFunctionCall(@src());

    var i: usize = MAX_ROWS;
    while (i > 0) : (i -= 1) {
        my_little_array[MAX_ROWS - i] = @intCast(i);
    }

    if (eflags & pg.EXEC_FLAG_EXPLAIN_ONLY == pg.EXEC_FLAG_EXPLAIN_ONLY) {
        pgzx.elog.Info(@src(), "explain mode", .{});
        return;
    }

    if (node.*.fdw_state == null) {
        var memctx = pgzx.mem.createAllocSetContext("zig_context", .{ .parent = node.*.ss.ps.state.*.es_query_cxt }) catch |err| {
            pgzx.elog.Warning(@src(), "failed to create memctx: {any}", .{err});
            @panic("failed to create memctx");
        };
        const allocator = memctx.allocator();

        memctx.registerAllocResetCallback(
            node.*.ss.ps.state.*.es_query_cxt,
            myCallback,
        ) catch |err| {
            pgzx.elog.Warning(@src(), "failed to register alloc reset callback: {any}", .{err});
        };

        const mss = allocator.create(MyScanState) catch |err| {
            pgzx.elog.Warning(@src(), "failed to allocate MyScanState {any}", .{err});
            @panic("failed to allocation MyScanState");
        };
        mss.current_row = 0;
        node.*.fdw_state = mss;
    } else {
        const mss: *MyScanState = @ptrCast(@alignCast(node.*.fdw_state));
        mss.current_row = 0;
    }
}

fn myCallback(ctx: pg.MemoryContext) void {
    _ = ctx;

    pgzx.elog.Info(@src(), "registerAllocResetCallback called", .{});
}

fn iterateForeignScan(node: [*c]pg.ForeignScanState) callconv(.C) [*c]pg.TupleTableSlot {
    logFunctionCall(@src());

    const slot = node.*.ss.ss_ScanTupleSlot;
    _ = pg.ExecClearTuple(slot);

    const my_scan_state: *MyScanState = @ptrCast(@alignCast(node.*.fdw_state));
    if (my_scan_state.current_row < MAX_ROWS) {
        const values = [_]pg.Datum{ pg.Int32GetDatum(my_little_array[@intCast(my_scan_state.current_row)]), pg.Int32GetDatum(my_scan_state.current_row + 2) };
        const values_ptr = @constCast(values[0..].ptr);

        const nulls = [_]bool{ false, false };
        const nulls_ptr = @constCast(nulls[0..].ptr);

        my_scan_state.current_row += 1;

        _ = pg.ExecStoreHeapTuple(pg.heap_form_tuple(slot.*.tts_tupleDescriptor, values_ptr, nulls_ptr), slot, false);
        return slot;
    }

    return null;
}

fn endForeignScan(node: [*c]pg.ForeignScanState) callconv(.C) void {
    logFunctionCall(@src());

    _ = node;
}

fn fdwSequentialIntsHandler() pg.Datum {
    const routines: *pg.FdwRoutine = makeFdwRoutineNode();
    routines.*.GetForeignRelSize = getForeignRelSize;
    routines.*.GetForeignPaths = getForeignPaths;
    // routines.*.GetForeignJoinPaths = getForeignJoinPaths;
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
