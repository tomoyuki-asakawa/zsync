#!/bin/sh

# 互換性情報
# このスクリプトは FreeBSD の sh、macOS の zsh、および bash との互換性があります。

# グローバル変数
VERSION="6.0"
SOURCE_SSH=""
SOURCE_DATASET=""
DESTINATION_SSH=""
DESTINATION_DATASET=""
SOURCE_PREFIX=""
DESTINATION_PREFIX=""
FULL_INCREMENTAL=0
REPLICATION=0
VERBOSE=0
VERBOSE_SEND=0
FORCE_RECEIVE=0
FORCE_SEND=0
CLEANUP_MODE=0
SHOW_DIFF_FILES=0    # -X に変更
DRY_RUN=0
ABORT_RECEIVE=0      # 新規追加 (-A)
KEEP_SNAPSHOTS=3
SOURCE_CMD=""
DESTINATION_CMD=""
SNAPSHOT_PREFIX=""
USE_IP=0
LARGE_BLOCK=0
COMPRESSED=0
EMBEDDED=0
PARSABLE=0
PROPS=0
NO_MOUNT=0

# シグナルハンドラ関数
handle_interrupt() {
    print_message "\nInterrupt received. Cleaning up and exiting..."
    exit 1
}

# シグナルをキャッチする
trap 'handle_interrupt' INT TERM

# メッセージ出力関数
print_message() {
    echo "$1" >&2
}

# Verboseメッセージ出力関数（VERBOSEモードがオンの場合のみ出力）
verbose_message() {
    if [ "$VERBOSE" -eq 1 ]; then
        echo "$1" >&2
    fi
}

# コマンドを実行し、結果とエラーコードを返す関数
execute_command_with_error() {
    local source_cmd="$1"
    local dest_cmd="$2"
    
    local full_cmd
    if [ -n "$source_cmd" ] && [ -n "$dest_cmd" ]; then
        full_cmd="$SOURCE_CMD $source_cmd | $DESTINATION_CMD $dest_cmd"
    elif [ -n "$source_cmd" ]; then
        full_cmd="$SOURCE_CMD $source_cmd"
    elif [ -n "$dest_cmd" ]; then
        full_cmd="$DESTINATION_CMD $dest_cmd"
    else
        print_message "エラー: コマンドが指定されていません。"
        return 1
    fi
    
    verbose_message "実行するコマンド: $full_cmd"
    
    eval "$full_cmd"
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        print_message "エラー: コマンド実行が失敗しました。終了コード: $exit_code"
        return $exit_code
    fi
}

execute_zfs_send_receive() {
    local send_cmd="$1"
    local receive_cmd="$2"
    
    if [ "$DRY_RUN" -eq 1 ]; then
        verbose_message "ドライラン: ZFS send/receive を実行します"
        verbose_message "送信コマンド: $send_cmd"
        verbose_message "受信コマンド: $receive_cmd (-> /dev/null)"
        execute_command_with_error "$send_cmd" "cat > /dev/null"
    else
        execute_command_with_error "$send_cmd" "$receive_cmd"
    fi
}

# ホスト識別子を取得する関数
get_host_identifier() {
    local user_host="$1"
    
    if [ -z "$user_host" ] || [ "$user_host" = "localhost" ]; then
        if [ "$USE_IP" = "1" ]; then
            hostname -I | awk '{print $1}'
        else
            hostname
        fi
    else
        if [ "$USE_IP" = "1" ]; then
            ssh "$user_host" "hostname -I | awk '{print \$1}'" 2>/dev/null
        else
            ssh "$user_host" hostname 2>/dev/null
        fi
    fi
}

# デフォルトプレフィックスを生成する関数
generate_default_prefix() {
    local source_host=$(get_host_identifier "$SOURCE_SSH")
    local dest_host=$(get_host_identifier "$DESTINATION_SSH")
    local sanitized_source_dataset=$(sanitize_name "$SOURCE_DATASET")
    local sanitized_dest_dataset=$(sanitize_name "$DESTINATION_DATASET")

    local prefix="zsync"

    if [ "$source_host" != "$dest_host" ]; then
        prefix="${prefix}-${source_host}-${dest_host}"
    fi

    if [ "$sanitized_source_dataset" != "$sanitized_dest_dataset" ]; then
        prefix="${prefix}-${sanitized_source_dataset}-${sanitized_dest_dataset}"
    fi

    echo "$prefix"
}

# 設定を初期化し、事前計算を行う関数
initialize_config() {
    parse_arguments "$@"
    
    if [ -n "$SOURCE_SSH" ]; then
        SOURCE_CMD="ssh $SOURCE_SSH sudo"
    else
        SOURCE_CMD="sudo"
    fi

    if [ -n "$DESTINATION_SSH" ]; then
        DESTINATION_CMD="ssh $DESTINATION_SSH sudo"
    else
        DESTINATION_CMD="sudo"
    fi

    if [ -z "$SOURCE_PREFIX" ]; then
        SOURCE_PREFIX=$(generate_default_prefix)
    fi
    SNAPSHOT_PREFIX="$SOURCE_PREFIX"

    if [ -z "$DESTINATION_PREFIX" ]; then
        DESTINATION_PREFIX="$SNAPSHOT_PREFIX"
    fi
}

# ソースとデスティネーションの情報を解析する関数
parse_source_destination() {
    SOURCE="$1"
    SOURCE_SSH=$(echo "$SOURCE" | cut -d: -f1)
    SOURCE_DATASET=$(echo "$SOURCE" | cut -d: -f2)
    if [ "$SOURCE" = "$SOURCE_DATASET" ]; then
        SOURCE_SSH=""
    fi

    DESTINATION="$2"
    DESTINATION_SSH=$(echo "$DESTINATION" | cut -d: -f1)
    DESTINATION_DATASET=$(echo "$DESTINATION" | cut -d: -f2)
    if [ "$DESTINATION" = "$DESTINATION_DATASET" ]; then
        DESTINATION_SSH=""
    fi
}

# 名前をサニタイズする関数
sanitize_name() {
    echo "$1" | sed 's/[^A-Za-z0-9._-]/_/g'
}

# スナップショット名を生成する関数
generate_snapshot_name() {
    local timestamp=$(date +%Y%m%d%H%M%S)
    echo "${SNAPSHOT_PREFIX}-${timestamp}"
}

# 最新のスナップショットを取得する関数
get_latest_snapshot() {
    verbose_message "最新スナップショットを取得: データセット=$DESTINATION_DATASET, プレフィックス=$DESTINATION_PREFIX"
    
    local cmd="zfs list -t snapshot -H -o name -s creation '$DESTINATION_DATASET' | grep '$DESTINATION_PREFIX' | tail -1 | cut -d'@' -f2"
    
    local result
    result=$(execute_command_with_error "" "$cmd")
    local exit_code=$?
    
    if [ $exit_code -ne 0 ] || [ -z "$result" ]; then
        verbose_message "最新スナップショットの取得に失敗しました。データセットにスナップショットが存在しない可能性があります。"
        return 1
    fi
    
    echo "$result"
}

# 対応するソーススナップショットを見つける関数
find_corresponding_source_snapshot() {
    local dest_snapshot="$1"
    local cmd="zfs list -t snapshot -H -o name | grep \"$SOURCE_DATASET@$dest_snapshot\""
    
    verbose_message "対応するソーススナップショットを確認: $cmd"
    
    local result=$(execute_command_with_error "$cmd" "")
    local exit_code=$?
    
    verbose_message "コマンド実行結果: $result (終了コード: $exit_code)"
    
    echo "$result"
}

# 新しいスナップショットを作成する関数
create_snapshot() {
    local snapshot_name=$(generate_snapshot_name)
    
    verbose_message "新しいスナップショットを作成します: ${SOURCE_DATASET}@${snapshot_name}"
    execute_command_with_error "zfs snapshot ${SOURCE_DATASET}@${snapshot_name}" ""
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        verbose_message "スナップショットを作成しました: ${SOURCE_DATASET}@${snapshot_name}"
        echo "$snapshot_name"
    else
        print_message "エラー: スナップショットの作成に失敗しました。"
        echo ""
    fi
}

abort_receive() {
    verbose_message "中断された受信状態を破棄します: ${DESTINATION_DATASET}"
    
    local receive_cmd="zfs receive -A"
    # ドライランの場合は -n を追加
    if [ "$DRY_RUN" -eq 1 ]; then
        receive_cmd="$receive_cmd -n"
    fi
    # verbose出力
    if [ "$VERBOSE" -eq 1 ] || [ "$VERBOSE_SEND" -eq 1 ]; then
        receive_cmd="$receive_cmd -v"
    fi

    execute_command_with_error "" "$receive_cmd ${DESTINATION_DATASET}"
}

# generate_send_options 関数を修正
generate_send_options() {
    local mode="$1"      # "incremental" or "full" or "resume"
    local base_cmd="zfs send"
    local options=""
    
    # verboseオプションの処理
    if [ "$VERBOSE" -eq 1 ] || [ "$VERBOSE_SEND" -eq 1 ]; then
        options="$options -v"
    fi

    # ドライランオプション
    if [ "$DRY_RUN" -eq 1 ]; then
        options="$options -n"
    fi

    # その他のオプション（変更なし）
    if [ "$PARSABLE" -eq 1 ]; then
        options="$options -P"
    fi
    if [ "$LARGE_BLOCK" -eq 1 ]; then
        options="$options -L"
    fi
    if [ "$COMPRESSED" -eq 1 ]; then
        options="$options -c"
    fi
    if [ "$EMBEDDED" -eq 1 ]; then
        options="$options -e"
    fi
    if [ "$PROPS" -eq 1 ]; then
        options="$options -p"
    fi
    if [ "$REPLICATION" -eq 1 ] && [ "$mode" != "resume" ]; then
        options="$options -R"
    fi

    case "$mode" in
        "incremental")
            if [ "$FULL_INCREMENTAL" -eq 1 ]; then
                options="$options -I"
            else
                options="$options -i"
            fi
            ;;
        "resume")
            options="$options -t"
            ;;
    esac

    echo "$base_cmd$options"
}

# receive オプション生成
generate_receive_options() {
    local base_cmd="zfs receive"
    local options=""

    # -s オプション（resumeサポート）は常に付与
    options="$options -s"
    
    # 強制オプション
    if [ "$FORCE_RECEIVE" -eq 1 ]; then
        options="$options -F"
    fi

    # マウントしないオプション
    if [ "$NO_MOUNT" -eq 1 ]; then
        options="$options -u"
    fi

    # ドライランの場合
    if [ "$DRY_RUN" -eq 1 ]; then
        options="$options -n"
    fi

    # verbose出力
    if [ "$VERBOSE" -eq 1 ] || [ "$VERBOSE_SEND" -eq 1 ]; then
        options="$options -v"
    fi

    echo "$base_cmd$options"
}

# フルセンド実行関数
perform_full_send() {
    local new_snapshot=$(create_snapshot)
    if [ -n "$new_snapshot" ]; then
        verbose_message "スナップショットのフルセンドを実行します: $new_snapshot"
        
        local send_cmd=$(generate_send_options "full")
        local receive_cmd=$(generate_receive_options)
        
        verbose_message "使用する送信オプション: $send_cmd"
        verbose_message "使用する受信オプション: $receive_cmd"

        send_cmd="$send_cmd ${SOURCE_DATASET}@${new_snapshot}"
        execute_zfs_send_receive "$send_cmd" "$receive_cmd ${DESTINATION_DATASET}"
    else
        print_message "エラー: 新しいスナップショットの作成に失敗しました。"
        return 1
    fi
}

# インクリメンタルセンド実行関数
perform_incremental_send() {
    local base_snapshot="$1"
    local current_snapshot="$2"

    if [ -z "$current_snapshot" ]; then
        print_message "エラー: 新しいスナップショットが提供されていません。"
        return 1
    fi

    local base_dataset=$(echo "$base_snapshot" | cut -d'@' -f1)
    local base_snapshot_name=$(echo "$base_snapshot" | cut -d'@' -f2)

    verbose_message "インクリメンタルセンドを実行: $base_snapshot から ${SOURCE_DATASET}@${current_snapshot} へ"
    
    local send_cmd=$(generate_send_options "incremental")
    local receive_cmd=$(generate_receive_options)
    
    verbose_message "使用する送信オプション: $send_cmd"
    verbose_message "使用する受信オプション: $receive_cmd"

    send_cmd="$send_cmd ${base_dataset}@${base_snapshot_name} ${SOURCE_DATASET}@${current_snapshot}"
    
    # 送信実行
    if ! execute_zfs_send_receive "$send_cmd" "$receive_cmd ${DESTINATION_DATASET}"; then
        if [ "$DRY_RUN" -eq 1 ]; then
            verbose_message "ドライラン: 転送シミュレーション完了"
            return 0
        else
            print_message "エラー: 転送に失敗しました。-F オプションが必要かもしれません。"
            return 1
        fi
    fi

    return 0
}

# スナップショットのリストを取得する関数を修正
get_snapshots_list() {
    local dataset="$1"
    local prefix="$2"
    local cmd="zfs list -t snapshot -H -o name -s creation | grep '${dataset}@${prefix}' | sort"
    
    verbose_message "スナップショットリスト取得コマンド: $cmd"
    
    if [ -n "$SOURCE_CMD" ] && [ "$3" = "source" ]; then
        execute_command_with_error "$cmd" ""
    elif [ -n "$DESTINATION_CMD" ] && [ "$3" = "destination" ]; then
        execute_command_with_error "" "$cmd"
    else
        print_message "エラー: 不明なコマンドタイプです"
        return 1
    fi
}

# cleanup_old_snapshots 関数を修正
cleanup_old_snapshots() {
    local dataset="$1"
    local prefix="$2"
    local type="$3"  # "source" または "destination"
    
    verbose_message "古いスナップショットの削除を開始: データセット=$dataset, プレフィックス=$prefix"
    
    local snapshots_list
    snapshots_list=$(get_snapshots_list "$dataset" "$prefix" "$type")
    if [ $? -ne 0 ]; then
        print_message "スナップショットリストの取得に失敗しました"
        return 1
    fi
    
    local total_snapshots=$(echo "$snapshots_list" | wc -l)
    
    verbose_message "合計スナップショット数: $total_snapshots"
    verbose_message "保持するスナップショット数: $KEEP_SNAPSHOTS"
    
    if [ "$total_snapshots" -le "$KEEP_SNAPSHOTS" ]; then
        verbose_message "削除する古いスナップショットはありません。"
        return 0
    fi
    
    local snapshots_to_delete=$(echo "$snapshots_list" | sort -r | tail -n +$((KEEP_SNAPSHOTS + 1)))
    local delete_count=$(echo "$snapshots_to_delete" | wc -l)
    
    verbose_message "削除対象のスナップショット数: $delete_count"
    verbose_message "削除対象のスナップショット:"
    echo "$snapshots_to_delete"
    
    local actually_deleted=0
    
    local old_IFS="$IFS"
    IFS=$'\n'
    for snapshot in $snapshots_to_delete; do
        verbose_message "処理中のスナップショット: $snapshot"
        local delete_cmd="zfs destroy $snapshot"
        
        if [ "$type" = "source" ]; then
            execute_command_with_error "$delete_cmd" ""
        else
            execute_command_with_error "" "$delete_cmd"
        fi
        
        if [ $? -eq 0 ]; then
            verbose_message "スナップショット $snapshot を削除しました。"
            actually_deleted=$((actually_deleted + 1))
        else
            print_message "エラー: スナップショット $snapshot の削除に失敗しました。"
        fi
    done
    IFS="$old_IFS"
    
    verbose_message "削除処理が完了しました。削除されたスナップショット数: $actually_deleted"
}

# perfom_cleanup 関数も修正
perfom_cleanup() {
    print_message "古いスナップショットの削除を開始します。"
    
    verbose_message "ソース側のクリーンアップを開始"
    cleanup_old_snapshots "$SOURCE_DATASET" "$SOURCE_PREFIX" "source"
    
    verbose_message "デスティネーション側のクリーンアップを開始"
    cleanup_old_snapshots "$DESTINATION_DATASET" "$DESTINATION_PREFIX" "destination"
    
    print_message "古いスナップショットの削除が完了しました。"
}

# エラー報告関数
report_error() {
    print_message "エラー: $1"
    exit 1
}

# 初期情報を表示する関数
display_initial_info() {
    verbose_message "Source      : ${SOURCE_SSH:+$SOURCE_SSH:}$SOURCE_DATASET@$SOURCE_PREFIX"
    verbose_message "Destination : ${DESTINATION_SSH:+$DESTINATION_SSH:}$DESTINATION_DATASET@$DESTINATION_PREFIX"
}

# レジュームトークンを取得する関数
get_resume_token() {
    verbose_message "デスティネーションデータセットのレジュームトークンを確認しています: $DESTINATION_DATASET"
    local cmd="zfs get -H -o value receive_resume_token $DESTINATION_DATASET"
    local result=$(execute_command_with_error "" "$cmd")
    local exit_code=$?
    
    if [ $exit_code -ne 0 ]; then
        verbose_message "レジュームトークンの取得エラー。終了コード: $exit_code"
        echo ""
    elif [ -z "$result" ] || [ "$result" = "-" ]; then
        verbose_message "有効なレジュームトークンが見つかりません。"
        echo ""
    else
        verbose_message "レジュームトークン: $result"
        echo "$result"
    fi
}

# レジューム転送を実行する関数
resume_transfer() {
    local resume_token="$1"
    if [ -n "$resume_token" ]; then
        verbose_message "レジュームトークンを使用して転送を再開します。"
        
        local send_cmd="$(generate_send_options "resume") $resume_token"
        local receive_cmd=$(generate_receive_options)
        
        verbose_message "使用する送信オプション: $send_cmd"
        verbose_message "使用する受信オプション: $receive_cmd"

        execute_zfs_send_receive "$send_cmd" "$receive_cmd ${DESTINATION_DATASET}"
        return 0
    else
        print_message "エラー: 無効なレジュームトークンです。"
        return 1
    fi
}

# 差分がなければ false を返す関数 オプションで　差分を表示。
check_snapshot_diff() {
    local previous_snapshot="$1"
    local current_snapshot="$2"
    local show_diff="$3"  # 差分表示オプション

    if [ -z "$previous_snapshot" ] || [ -z "$current_snapshot" ]; then
        print_message "エラー: 差分チェックのために両方のスナップショットが必要です。"
        return 1
    fi
    
    local diff_cmd="zfs diff -H ${SOURCE_DATASET}@${previous_snapshot} ${SOURCE_DATASET}@${current_snapshot}"
    verbose_message "差分チェックコマンド: $diff_cmd"

    local diff_output=$(execute_command_with_error "$diff_cmd" "")
    local has_diff=0
    
    if [ -z "$diff_output" ]; then
        verbose_message "差分はありません"
        has_diff=1
    else
        verbose_message "差分が検出されました"
        if [ "$show_diff" -eq 1 ]; then
            print_message "検出された差分:"
            echo "$diff_output"
        fi
    fi
    
    return $has_diff
}

# ソースのスナップショットを削除する関数
delete_source_snapshot() {
    local snapshot_name="$1"
    verbose_message "ソースのスナップショットを削除します: $snapshot_name"
    execute_command_with_error "zfs destroy $snapshot_name" ""
}

# ディスティネーションのスナップショットを削除する関数
delete_destination_snapshot() {
    local snapshot_name="$1"
    verbose_message "ディスティネーションのスナップショットを削除します: $snapshot_name"
    execute_command_with_error "" "zfs destroy $snapshot_name"
}


# 引数をパースし、設定を更新する関数
parse_arguments() {
    while getopts "Afs:d:IVvFCk:XniRLcepPu" opt; do
        case "$opt" in
            A) ABORT_RECEIVE=1 ;;     # 新規追加
            f) FORCE_SEND=1 ;;
            s) SOURCE_PREFIX="$OPTARG" ;;
            d) DESTINATION_PREFIX="$OPTARG" ;;
            I) FULL_INCREMENTAL=1 ;;
            V) VERBOSE=1 ;;
            v) VERBOSE_SEND=1 ;;
            F) FORCE_RECEIVE=1 ;;
            C) CLEANUP_MODE=1 ;;
            k) KEEP_SNAPSHOTS="$OPTARG" ;;
            X) SHOW_DIFF_FILES=1 ;;   # -D から -X に変更
            n) DRY_RUN=1 ;;
            i) USE_IP=1 ;;
            R) REPLICATION=1 ;;
            L) LARGE_BLOCK=1 ;;
            c) COMPRESSED=1 ;;
            e) EMBEDDED=1 ;;
            p) PROPS=1 ;;
            P) PARSABLE=1 ;;
            u) NO_MOUNT=1 ;;
            *) usage ;;
        esac
    done
    shift $((OPTIND -1))

    if [ $# -ne 2 ]; then
        usage
    fi

    parse_source_destination "$1" "$2"
}

# 使用方法を表示する関数
usage() {
    echo "使用方法: $0 [オプション] source_user@source_host:source_dataset destination_user@destination_host:destination_dataset" >&2
    echo "" >&2
    echo "ZSync固有のオプション:" >&2
    echo "  -f                既存のスナップショットにかかわらずフルセンドを強制する (デフォルト: 無効)" >&2
    echo "  -s snapshot_prefix ソーススナップショットプレフィックスを設定" >&2
    echo "                    (デフォルト: 'zsync-<source>-<dest>-<dataset>')" >&2
    echo "  -d snapshot_prefix デスティネーションスナップショットプレフィックスを設定" >&2
    echo "                    (デフォルト: ソースと同じ)" >&2
    echo "  -V                Verboseモードを有効にする (デフォルト: 無効)" >&2
    echo "  -C                クリーンアップモードを有効にする (デフォルト: 無効)" >&2
    echo "  -k number         保持するスナップショットの数 (デフォルト: 3)" >&2
    echo "  -X                差分ファイルを表示する (デフォルト: 無効)" >&2
    echo "  -i                IPアドレスを使用する (デフォルト: ホスト名を使用)" >&2
    echo "" >&2
    echo "ZFS送信オプション:" >&2
    echo "  -I                完全インクリメンタルセンド（中間スナップショットを含む）を実行する" >&2
    echo "                    (デフォルト: 最新スナップショットからの差分のみ)" >&2
    echo "  -R                レプリケーションストリーム（-R）を使用する (デフォルト: 無効)" >&2
    echo "  -v                send コマンドの verbose (-v) を有効にする (デフォルト: 無効)" >&2
    echo "  -n                送信のドライラン。実際のデータは送信せず情報のみ表示 (デフォルト: 無効)" >&2
    echo "  -L                128KB以上のブロックサイズを許可する (デフォルト: 無効)" >&2
    echo "  -c                圧縮されたWRITEレコードを使用する (デフォルト: 無効)" >&2
    echo "  -e                可能な場合は埋め込みデータを使用する (デフォルト: 無効)" >&2
    echo "  -p                データセットのプロパティを含める (デフォルト: 無効)" >&2
    echo "  -P                マシンパース可能な詳細出力を生成する (デフォルト: 無効)" >&2
    echo "" >&2
    echo "ZFS受信オプション:" >&2
    echo "  -A                中断された受信の状態を破棄する (デフォルト: 無効)" >&2
    echo "  -F                receive の強制 (-F) オプションを有効にする (デフォルト: 無効)" >&2
    echo "  -u                受信後にファイルシステムをマウントしない (デフォルト: マウントする)" >&2
    echo "" >&2
    echo "デフォルトの動作:" >&2
    echo "  - 増分転送を優先（デスティネーションに既存のスナップショットがある場合）" >&2
    echo "  - スナップショットの自動生成とクリーンアップ" >&2
    echo "  - 受信時の resume サポート (-s) は常に有効" >&2
    echo "  - 転送失敗時は部分的な受信状態を保持" >&2
    echo "" >&2
    echo "例:" >&2
    echo "  $0 root@source:zroot/data root@backup:backup/data    # 基本的な使用法" >&2
    echo "  $0 -R -c -L source:tank/vm backup:backup/vm          # 圧縮とレプリケーション" >&2
    echo "  $0 -n -v source:data backup:archive                  # 送信のドライラン（詳細表示）" >&2
    echo "  $0 -A backup:incomplete                              # 中断された受信を破棄" >&2
    echo "" >&2
    echo "このスクリプトは FreeBSD sh、macOS zsh、bash と互換性があります。" >&2
    exit 1
}

# バージョン情報を表示する関数
show_version() {
    echo "ZSync スクリプトバージョン $VERSION" >&2
    echo "FreeBSD sh、macOS zsh、bash と互換性があります。" >&2
    exit 0
}

# メイン処理関数
main() {
    initialize_config "$@"
    
    if [ "$VERBOSE" -eq 1 ]; then
        display_initial_info
    fi
    
    # 受信中断状態の破棄処理 
    if [ "$ABORT_RECEIVE" -eq 1 ]; then
        abort_receive
        return $?
    fi
    
    if [ "$DRY_RUN" -eq 1 ]; then
        print_message "ドライランモード: 実際の変更は行われません。"
    fi

    local resume_token=$(get_resume_token)
    
    if [ -n "$resume_token" ]; then
        verbose_message "レジュームトークンを使用して転送を再開します。"
        resume_transfer "$resume_token"
    else
        local dest_latest=$(get_latest_snapshot)
        
        if [ -z "$dest_latest" ] || [ "$FORCE_SEND" -eq 1 ]; then
            if [ "$FORCE_SEND" -eq 1 ]; then
                verbose_message "フルセンドが強制されました。"
            else
                verbose_message "デスティネーションに既存のスナップショットがありません。フルセンドを実行します。"
            fi
            perform_full_send
        else
            verbose_message "デスティネーションの最新スナップショット: $dest_latest"
            local corresponding_source=$(find_corresponding_source_snapshot "$dest_latest")
            if [ -z "$corresponding_source" ]; then
                verbose_message "対応するソーススナップショットが見つかりません: $dest_latest"
                verbose_message "フルセンドを実行します。"
                perform_full_send
            else
                verbose_message "対応するソーススナップショットが見つかりました: $corresponding_source"
                
                # 新しいスナップショットを一度だけ作成
                local current_snapshot=$(create_snapshot)

                local previous_snapshot=$(get_latest_snapshot)
                
                # SHOW_DIFF_FILES の値に応じて差分表示
                if ! check_snapshot_diff "$previous_snapshot" "$current_snapshot" "$SHOW_DIFF_FILES"; then
                    verbose_message "差分がないため、インクリメンタルセンドをスキップし、スナップショットを削除します。"
                    # 差分がない場合は常に　ソース側の current_snapshot　を削除
                    delete_source_snapshot "${SOURCE_DATASET}@${current_snapshot}"
                    return 0
                fi

                                verbose_message "インクリメンタルセンドを実行します。"
                if ! perform_incremental_send "$corresponding_source" "$current_snapshot"; then
                    if [ "$DRY_RUN" -eq 1 ]; then
                        verbose_message "ドライラン: 転送シミュレーション完了"
                        return 0
                    else
                        print_message "エラー: 転送に失敗しました。-F オプションが必要かもしれません。"
                        # 失敗した場合は current_snapshot のみを削除し、previous は保持
                        delete_source_snapshot "${SOURCE_DATASET}@${current_snapshot}"
                        return 1
                    fi
                fi

                # 転送成功時のみ previous を削除
                if [ "$DRY_RUN" -eq 0 ]; then
                    verbose_message "転送が成功したため、前のスナップショットを削除します: ${previous_snapshot}"
                    delete_source_snapshot "${SOURCE_DATASET}@${previous_snapshot}"
                    delete_destination_snapshot "${DESTINATION_DATASET}@${previous_snapshot}"
                fi
            fi
        fi
    fi

    # クリーンアップ処理
    if [ "$CLEANUP_MODE" -eq 1 ]; then
        perfom_cleanup
    fi
        
    if [ "$DRY_RUN" -eq 1 ]; then
        print_message "ドライランが完了しました。変更は行われていません。"
    elif [ "$VERBOSE" -eq 1 ]; then
        print_message "ZSync操作が完了しました。"
    fi
}


# スクリプトの実行
main "$@"