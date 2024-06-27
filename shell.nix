# `nix-shell` to get examples and tests runnable without any manual rust or c-dependnecies installation on linux and mac
{ pkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/nixos-24.05.tar.gz") { } }:
let
  fenix = import (fetchTarball "https://github.com/nix-community/fenix/archive/main.tar.gz") { };
in
pkgs.mkShell rec {
  name = "redb";
  nativeBuildInputs = [
    (fenix.fromToolchainFile { dir = ./.; })
    pkgs.clang
  ] ++ pkgs.lib.optionals pkgs.stdenv.isLinux [ pkgs.pkg-config ];

  buildInputs = with pkgs; [
    libclang.lib
    rocksdb
  ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
    darwin.apple_sdk.frameworks.SystemConfiguration
  ];
  shellHook = ''
    export LIBCLANG_PATH="${pkgs.libclang.lib}/lib";
    export ROCKSDB_LIB_DIR="${pkgs.rocksdb}/lib";
  '';
}
