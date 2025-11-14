import argparse
import os
import sys
import numpy as np
import open3d as o3d
import random

# INFO PRINTERS
def print_mesh_info(mesh, label="Mesh"):
    vertices = len(mesh.vertices)
    triangles = len(mesh.triangles)
    has_colors = mesh.has_vertex_colors()
    has_normals = mesh.has_vertex_normals()
    print(f"{label} info:")
    print(f"  vertices: {vertices:,}")
    print(f"  triangles: {triangles:,}")
    print(f"  has colors: {has_colors}")
    print(f"  has normals: {has_normals}")

def print_pcd_info(pcd, label="PointCloud"):
    points = len(pcd.points)
    has_colors = pcd.has_colors()
    has_normals = pcd.has_normals()
    print(f"{label} info:")
    print(f"  points: {points:,}")
    print(f"  has colors: {has_colors}")
    print(f"  has normals: {has_normals}")

def print_voxel_info(vg, label="VoxelGrid"):
    n = len(vg.get_voxels())
    print(f"{label} info:")
    print(f"  voxel count: {n:,}")

# MESH LOADING (PLY ONLY)
def load_mesh_ply(model_path):
    if not os.path.exists(model_path):
        print(f"ERROR: File not found: {model_path}")
        print("  Make sure your .ply file is in the correct folder.")
        print("  Example: --model 'hollow_knight_clean.ply'")
        sys.exit(1)

    print(f"Loading PLY: {model_path}")
    mesh = o3d.io.read_triangle_mesh(model_path)

    if mesh.is_empty():
        raise ValueError(f"PLY file is empty or invalid: {model_path}")

    # Clean and fix
    mesh.remove_duplicated_vertices()
    mesh.remove_duplicated_triangles()
    mesh.remove_non_manifold_edges()
    if not mesh.has_vertex_normals():
        mesh.compute_vertex_normals()

    print_mesh_info(mesh, "Loaded PLY Mesh")
    return mesh

# AUTO-SCALE & ORIENT
def auto_scale_and_orient(mesh, target_height=1.8, invert_rotate=False):
    bbox = mesh.get_axis_aligned_bounding_box()
    extents = np.array(bbox.get_extent())
    print(f"  Extents BEFORE (X,Y,Z): {extents}")

    # Scale
    current_height = max(extents)  # Largest dim as height
    scale = target_height / current_height if current_height > 0.001 else 1.0
    mesh.scale(scale, center=bbox.get_center())
    print(f"  Scaled: {scale:.2f}x")

    # Rotate: detect tallest axis
    bbox2 = mesh.get_axis_aligned_bounding_box()
    extents2 = np.array(bbox2.get_extent())
    tallest_idx = np.argmax(extents2)
    print(f"  Tallest axis: {['X', 'Y', 'Z'][tallest_idx]}")

    if tallest_idx == 2:  # Z tallest → rotate to Y
        angle = -np.pi / 2 if invert_rotate else np.pi / 2
        R = o3d.geometry.get_rotation_matrix_from_xyz((angle, 0, 0))
        mesh.rotate(R, center=mesh.get_center())
        print(f"  Rotated {'-90°' if invert_rotate else '+90°'} on X (Z→Y)")
    elif tallest_idx == 0:  # X tallest → rotate to Y
        angle = np.pi / 2
        R = o3d.geometry.get_rotation_matrix_from_xyz((0, 0, angle))
        mesh.rotate(R, center=mesh.get_center())
        print("  Rotated X→Y")
    else:
        print("  Already Y-up ✓")

    final_extents = np.array(mesh.get_axis_aligned_bounding_box().get_extent())
    print(f"  Extents AFTER (X,Y,Z): {final_extents}")
    return mesh

# GRADIENT & EXTREMA

def apply_gradient(mesh, axis=1):
    verts = np.asarray(mesh.vertices)
    vals = verts[:, axis]
    mn, mx = vals.min(), vals.max()
    if mx - mn < 1e-6:
        normalized = np.zeros_like(vals)
    else:
        normalized = (vals - mn) / (mx - mn)
    colors = np.zeros((len(verts), 3))
    colors[:, 0] = normalized      # Red: high
    colors[:, 2] = 1 - normalized  # Blue: low
    mesh.vertex_colors = o3d.utility.Vector3dVector(colors)

def highlight_extrema(mesh, axis=1, radius=0.03):
    verts = np.asarray(mesh.vertices)
    vals = verts[:, axis]
    i_min, i_max = np.argmin(vals), np.argmax(vals)
    p_min, p_max = verts[i_min], verts[i_max]
    s_min = o3d.geometry.TriangleMesh.create_sphere(radius=radius)
    s_max = o3d.geometry.TriangleMesh.create_sphere(radius=radius)
    s_min.translate(p_min); s_min.paint_uniform_color([0, 1, 0])  # Green
    s_max.translate(p_max); s_max.paint_uniform_color([1, 0, 0])  # Red
    return p_min, p_max, s_min, s_max

# CLIPPING
def clip_mesh(mesh, point, normal, keep_left=True):
    verts = np.asarray(mesh.vertices)
    tris = np.asarray(mesh.triangles)
    centers = np.mean(verts[tris], axis=1)
    signed = np.dot(centers - point, normal)
    mask = signed <= 0 if keep_left else signed >= 0
    kept_tris = tris[mask]
    if len(kept_tris) == 0:
        return o3d.geometry.TriangleMesh()

    uniq, inv = np.unique(kept_tris, return_inverse=True)
    new_verts = verts[uniq]
    new_tris = inv.reshape(-1, 3)

    new_mesh = o3d.geometry.TriangleMesh()
    new_mesh.vertices = o3d.utility.Vector3dVector(new_verts)
    new_mesh.triangles = o3d.utility.Vector3iVector(new_tris)
    if mesh.has_vertex_colors():
        new_mesh.vertex_colors = o3d.utility.Vector3dVector(np.asarray(mesh.vertex_colors)[uniq])
    new_mesh.compute_vertex_normals()
    return new_mesh

# MAIN
def main():
    parser = argparse.ArgumentParser(description="Hollow Knight PLY Pipeline")
    parser.add_argument('--model', type=str, default='hollow_knight_clean.ply',
                        help='Path to .ply file')
    parser.add_argument('--voxel_size', type=float, default=0.0,
                        help='Voxel size (0 = auto)')
    parser.add_argument('--axis', type=str, default='y', choices=['x','y','z'],
                        help='Clip & gradient axis')
    parser.add_argument('--keep', type=str, default='left', choices=['left','right'],
                        help='Side to keep after clip')
    parser.add_argument('--headless', action='store_true',
                        help='No visualization windows')
    parser.add_argument('--output', type=str, default='output_ply',
                        help='Output folder')
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    print("Hollow Knight PLY Pipeline")
    print(f"Model: {args.model}")
    print(f"Axis: {args.axis.upper()} | Keep: {args.keep}")

    # Load
    mesh = load_mesh_ply(args.model)
    mesh = auto_scale_and_orient(mesh)

    bbox = mesh.get_axis_aligned_bounding_box()
    center = bbox.get_center()
    diag = np.linalg.norm(bbox.get_extent())
    voxel_size = args.voxel_size if args.voxel_size > 0 else diag / 120
    print(f"  Voxel size: {voxel_size:.4f}")

    axis_idx = {'x':0, 'y':1, 'z':2}[args.axis]
    normal = np.zeros(3); normal[axis_idx] = 1.0

    # 1. Original
    if not args.headless:
        o3d.visualization.draw_geometries([mesh], window_name="1. Original")

    # 2. Point Cloud
    pcd = mesh.sample_points_uniformly(50000)
    pcd.estimate_normals()
    if not args.headless:
        o3d.visualization.draw_geometries([pcd], window_name="2. Point Cloud")
    print_pcd_info(pcd)

    # 3. Poisson
    poisson, _ = o3d.geometry.TriangleMesh.create_from_point_cloud_poisson(pcd, depth=9)
    poisson = poisson.crop(bbox.scale(1.1, center))
    poisson.compute_vertex_normals()
    if not args.headless:
        o3d.visualization.draw_geometries([poisson], window_name="3. Poisson")
    print_mesh_info(poisson, "Poisson")

    # 4. Voxel Art
    vg = o3d.geometry.VoxelGrid.create_from_point_cloud(pcd, voxel_size)
    voxels = vg.get_voxels()
    max_show = 3000
    show_voxels = random.sample(voxels, min(len(voxels), max_show)) if len(voxels) > max_show else voxels
    vox_meshes = []
    for i, v in enumerate(show_voxels):
        c = np.asarray(vg.get_voxel_center_coordinate(v.grid_index))
        cube = o3d.geometry.TriangleMesh.create_box(voxel_size, voxel_size, voxel_size)
        cube.translate(c - voxel_size/2)
        cube.compute_vertex_normals()
        intensity = 0.3 + 0.7 * (i % 5)/4
        cube.paint_uniform_color([intensity*0.3, intensity*0.1, intensity*0.2])
        vox_meshes.append(cube)
    if not args.headless and vox_meshes:
        o3d.visualization.draw_geometries(vox_meshes, window_name="4. Voxel Art")
    print_voxel_info(vg)

    # 5. Dynamic Cutting Plane
    axis_idx = {'x': 0, 'y': 1, 'z': 2}[args.axis]
    extents = bbox.get_extent()

    # Thin in cutting direction, large in others
    width = 0.005 if axis_idx == 0 else extents[0] * 1.5
    height = 0.005 if axis_idx == 1 else extents[1] * 1.5
    depth = 0.005 if axis_idx == 2 else extents[2] * 1.5

    plane = o3d.geometry.TriangleMesh.create_box(width, height, depth)
    plane.paint_uniform_color([0.9, 0.1, 0.1])  # Red
    plane.compute_vertex_normals()

    # Position at center
    plane_center_offset = np.array([width, height, depth]) / 2
    plane.translate(center - plane_center_offset)

    if not args.headless:
        o3d.visualization.draw_geometries([mesh, plane], window_name="5. With Cutting Plane")

    # 6. Clip
    clipped = clip_mesh(mesh, center, normal, keep_left=args.keep=='left')
    if clipped.is_empty():
        print("Warning: Clipping removed everything!")
        clipped = mesh
    if not args.headless:
        o3d.visualization.draw_geometries([clipped], window_name="6. Clipped")
    print_mesh_info(clipped, "Clipped")

    # 7. Final Gradient
    final = clipped.crop(clipped.get_axis_aligned_bounding_box())
    final.vertex_colors = o3d.utility.Vector3dVector(np.zeros((len(final.vertices), 3)))
    apply_gradient(final, axis_idx)
    p_min, p_max, s_min, s_max = highlight_extrema(final, axis_idx, voxel_size*2)
    print(f"  Min point ({args.axis}): {p_min}")
    print(f"  Max point ({args.axis}): {p_max}")
    if not args.headless:
        o3d.visualization.draw_geometries([final, s_min, s_max], window_name="7. Final Gradient")

    # Save all
    o3d.io.write_triangle_mesh(f"{args.output}/01_original.ply", mesh)
    o3d.io.write_triangle_mesh(f"{args.output}/03_poisson.ply", poisson)
    o3d.io.write_triangle_mesh(f"{args.output}/06_clipped.ply", clipped)
    o3d.io.write_triangle_mesh(f"{args.output}/07_final.ply", final)
    o3d.io.write_point_cloud(f"{args.output}/02_pcd.ply", pcd)
    print(f"\nAll files saved to: {args.output}/")

if __name__ == '__main__':
    main()