import { useEffect, useState, useRef, useCallback } from 'react'
import { useParams } from 'react-router-dom'
import {
  Table,
  Button,
  Input,
  InputNumber,
  Select,
  Tag,
  Space,
  Modal,
  Form,
  message,
  Popconfirm,
  Dropdown,
  Typography,
} from 'antd'
import type { MenuProps } from 'antd'
import {
  ReloadOutlined,
  CopyOutlined,
  LinkOutlined,
  PlusOutlined,
  DownloadOutlined,
  UploadOutlined,
  MoreOutlined,
  DeleteOutlined,
} from '@ant-design/icons'
import { apiFetch, API_BASE } from '@/lib/utils'
import { normalizeExecutorForPlatform } from '@/lib/registerOptions'

const { Text } = Typography

const STATUS_COLORS: Record<string, string> = {
  registered: 'default',
  trial: 'success',
  subscribed: 'success',
  expired: 'warning',
  invalid: 'error',
}

function LogPanel({ taskId, onDone }: { taskId: string; onDone: () => void }) {
  const [lines, setLines] = useState<string[]>([])
  const [done, setDone] = useState(false)
  const bottomRef = useRef<HTMLDivElement>(null)
  const handleCopyAll = async () => {
    try {
      await navigator.clipboard.writeText(lines.join('\n'))
      message.success('日志已复制')
    } catch {
      message.error('复制失败')
    }
  }

  useEffect(() => {
    if (!taskId) return
    const es = new EventSource(`${API_BASE}/tasks/${taskId}/logs/stream`)
    es.onmessage = (e) => {
      const d = JSON.parse(e.data)
      if (d.line) setLines((prev) => [...prev, d.line])
      if (d.done) {
        setDone(true)
        es.close()
        onDone()
      }
    }
    es.onerror = () => es.close()
    return () => es.close()
  }, [taskId])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [lines])

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 8 }}>
        <Button size="small" icon={<CopyOutlined />} onClick={handleCopyAll} disabled={lines.length === 0}>
          复制日志
        </Button>
      </div>
      <div
        className="log-panel"
        style={{
          flex: 1,
          overflow: 'auto',
          background: '#ffffff',
          border: '1px solid #e5e7eb',
          borderRadius: 8,
          padding: 12,
          fontFamily: 'monospace',
          fontSize: 12,
          minHeight: 200,
          maxHeight: 400,
          userSelect: 'text',
          WebkitUserSelect: 'text',
          cursor: 'text',
          whiteSpace: 'pre-wrap',
        }}
      >
        {lines.length === 0 && <div style={{ color: '#9ca3af' }}>等待日志...</div>}
        {lines.map((l, i) => (
          <div
            key={i}
            style={{
              lineHeight: 1.5,
              color: l.includes('✓') || l.includes('成功') ? '#059669' : l.includes('✗') || l.includes('失败') || l.includes('错误') ? '#dc2626' : '#1f2937',
            }}
          >
            {l}
          </div>
        ))}
        <div ref={bottomRef} />
      </div>
      {done && <div style={{ fontSize: 12, color: '#10b981', marginTop: 8 }}>注册完成</div>}
    </div>
  )
}

function ActionMenu({ acc, onRefresh }: { acc: any; onRefresh: () => void }) {
  const [actions, setActions] = useState<any[]>([])

  useEffect(() => {
    apiFetch(`/actions/${acc.platform}`)
      .then((d) => setActions(d.actions || []))
      .catch(() => {})
  }, [acc.platform])

  const handleAction = async (actionId: string) => {
    try {
      const r = await apiFetch(`/actions/${acc.platform}/${acc.id}/${actionId}`, {
        method: 'POST',
        body: JSON.stringify({ params: {} }),
      })
      if (!r.ok) {
        message.error(r.error || '操作失败')
        return
      }
      const data = r.data || {}
      if (data.url || data.checkout_url || data.cashier_url) {
        window.open(data.url || data.checkout_url || data.cashier_url, '_blank')
      } else {
        message.success(data.message || '操作成功')
      }
      onRefresh()
    } catch {
      message.error('请求失败')
    }
  }

  const menuItems: MenuProps['items'] = actions.map((a) => ({
    key: a.id,
    label: a.label,
    onClick: () => handleAction(a.id),
  }))

  if (actions.length === 0) return null

  return (
    <Dropdown menu={{ items: menuItems }}>
      <Button type="link" size="small" icon={<MoreOutlined />} />
    </Dropdown>
  )
}

export default function Accounts() {
  const { platform } = useParams<{ platform: string }>()
  const [currentPlatform, setCurrentPlatform] = useState(platform || 'trae')
  const isChatGPT = currentPlatform === 'chatgpt'
  const [accounts, setAccounts] = useState<any[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(false)
  const [search, setSearch] = useState('')
  const [filterStatus, setFilterStatus] = useState('')
  const [selectedRowKeys, setSelectedRowKeys] = useState<React.Key[]>([])

  const [registerModalOpen, setRegisterModalOpen] = useState(false)
  const [addModalOpen, setAddModalOpen] = useState(false)
  const [importModalOpen, setImportModalOpen] = useState(false)
  const [detailModalOpen, setDetailModalOpen] = useState(false)
  const [currentAccount, setCurrentAccount] = useState<any>(null)

  const [registerForm] = Form.useForm()
  const [addForm] = Form.useForm()
  const [detailForm] = Form.useForm()
  const [importText, setImportText] = useState('')
  const [importLoading, setImportLoading] = useState(false)
  const [taskId, setTaskId] = useState<string | null>(null)
  const [registerLoading, setRegisterLoading] = useState(false)
  const [deleteAllLoading, setDeleteAllLoading] = useState(false)
  const [check401Workers, setCheck401Workers] = useState(8)
  const [checking401Scope, setChecking401Scope] = useState<'all' | 'selected' | null>(null)
  const [deletingInvalid401, setDeletingInvalid401] = useState(false)
  const [cpaUploadingScope, setCpaUploadingScope] = useState<'all' | 'selected' | null>(null)
  const [check401Result, setCheck401Result] = useState<{
    title: string
    total: number
    valid: number
    invalid401: number
    failed: number
    workers: number
    items: Array<{ id?: number; email?: string; ok: boolean; invalid_401: boolean; status_code?: number | null; msg: string }>
  } | null>(null)
  const [cpaResult, setCpaResult] = useState<{
    title: string
    total: number
    success: number
    failed: number
    items: Array<{ id?: number; email?: string; ok: boolean; msg: string }>
  } | null>(null)

  useEffect(() => {
    if (platform) setCurrentPlatform(platform)
  }, [platform])

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const params = new URLSearchParams({ platform: currentPlatform, page: '1', page_size: '100' })
      if (search) params.set('email', search)
      if (filterStatus) params.set('status', filterStatus)
      const data = await apiFetch(`/accounts?${params}`)
      setAccounts(data.items)
      setTotal(data.total)
    } finally {
      setLoading(false)
    }
  }, [currentPlatform, search, filterStatus])

  useEffect(() => {
    load()
  }, [load])

  const copyText = (text: string) => {
    navigator.clipboard.writeText(text)
    message.success('已复制')
  }

  const exportCsv = () => {
    const header = 'email,password,status,region,cashier_url,created_at'
    const rows = accounts.map((a) => [a.email, a.password, a.status, a.region, a.cashier_url, a.created_at].join(','))
    const blob = new Blob([[header, ...rows].join('\n')], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `${currentPlatform}_accounts.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  const handleDelete = async (id: number) => {
    await apiFetch(`/accounts/${id}`, { method: 'DELETE' })
    message.success('删除成功')
    load()
  }

  const handleBatchDelete = async () => {
    if (selectedRowKeys.length === 0) return
    await apiFetch('/accounts/batch-delete', {
      method: 'POST',
      body: JSON.stringify({ ids: Array.from(selectedRowKeys) }),
    })
    message.success('批量删除成功')
    setSelectedRowKeys([])
    load()
  }

  const handleDeleteAll = async () => {
    setDeleteAllLoading(true)
    try {
      const result = await apiFetch('/accounts/delete-all', {
        method: 'POST',
        body: JSON.stringify({
          platform: currentPlatform,
          status: filterStatus || undefined,
          email: search || undefined,
        }),
      })
      message.success(`已删除 ${result.deleted} 个账号`)
      setSelectedRowKeys([])
      load()
    } catch (e: any) {
      let errorText = e?.message || '删除失败'
      try {
        const parsed = JSON.parse(errorText)
        errorText = parsed.detail || errorText
      } catch {
        // noop
      }
      message.error(`全部删除失败: ${errorText}`)
    } finally {
      setDeleteAllLoading(false)
    }
  }

  const handleAdd = async () => {
    const values = await addForm.validateFields()
    await apiFetch('/accounts', {
      method: 'POST',
      body: JSON.stringify({ ...values, platform: currentPlatform }),
    })
    message.success('添加成功')
    setAddModalOpen(false)
    addForm.resetFields()
    load()
  }

  const handleImport = async () => {
    if (!importText.trim()) return
    setImportLoading(true)
    try {
      const lines = importText.trim().split('\n').filter(Boolean)
      const res = await apiFetch('/accounts/import', {
        method: 'POST',
        body: JSON.stringify({ platform: currentPlatform, lines }),
      })
      message.success(`导入成功 ${res.created} 个`)
      setImportModalOpen(false)
      setImportText('')
      load()
    } catch (e: any) {
      message.error(`导入失败: ${e.message}`)
    } finally {
      setImportLoading(false)
    }
  }

  const handleCheck401 = async (scope: 'all' | 'selected') => {
    if (!isChatGPT) return

    const isSelectedScope = scope === 'selected'
    const ids = isSelectedScope ? selectedRowKeys.map((key) => Number(key)).filter((id) => Number.isInteger(id) && id > 0) : []

    if (isSelectedScope && ids.length === 0) {
      message.warning('请先选择要检测的账号')
      return
    }

    setChecking401Scope(scope)
    try {
      const payload = isSelectedScope
        ? { ids, workers: check401Workers }
        : {
            status: filterStatus || undefined,
            email: search || undefined,
            workers: check401Workers,
          }

      const result = await apiFetch('/accounts/chatgpt/check-401', {
        method: 'POST',
        body: JSON.stringify(payload),
      })

      setCheck401Result({
        title: isSelectedScope ? `检测选中的 ${result.total} 个 ChatGPT 账号 401` : `整体检测 ${result.total} 个 ChatGPT 账号 401`,
        total: result.total,
        valid: result.valid,
        invalid401: result.invalid_401,
        failed: result.failed,
        workers: Number(result.workers || check401Workers),
        items: result.items || [],
      })

      if (result.invalid_401 > 0) {
        message.warning(`401 检测完成：命中 ${result.invalid_401} 个，异常 ${result.failed} 个`)
      } else if (result.failed > 0) {
        message.warning(`401 检测完成：有效 ${result.valid} 个，异常 ${result.failed} 个`)
      } else {
        message.success(`401 检测完成：全部 ${result.valid} 个账号均正常`)
      }

      await load()
    } catch (e: any) {
      let errorText = e?.message || '检测失败'
      try {
        const parsed = JSON.parse(errorText)
        errorText = parsed.detail || errorText
      } catch {
        // noop
      }
      message.error(`401 检测失败: ${errorText}`)
    } finally {
      setChecking401Scope(null)
    }
  }

  const handleDeleteInvalid401 = async () => {
    if (!isChatGPT) return

    const ids = selectedRowKeys.map((key) => Number(key)).filter((id) => Number.isInteger(id) && id > 0)
    const useSelected = ids.length > 0

    setDeletingInvalid401(true)
    try {
      const payload = useSelected
        ? { ids }
        : {
            status: filterStatus || undefined,
            email: search || undefined,
          }

      const result = await apiFetch('/accounts/chatgpt/delete-invalid-401', {
        method: 'POST',
        body: JSON.stringify(payload),
      })

      if (result.deleted > 0) {
        message.success(`已删除 ${result.deleted} 个 401 无效账号，跳过 ${result.skipped} 个`)
      } else {
        message.warning('当前范围内没有可删除的 401 无效账号')
      }

      setSelectedRowKeys([])
      await load()
    } catch (e: any) {
      let errorText = e?.message || '删除失败'
      try {
        const parsed = JSON.parse(errorText)
        errorText = parsed.detail || errorText
      } catch {
        // noop
      }
      message.error(`删除 401 无效账号失败: ${errorText}`)
    } finally {
      setDeletingInvalid401(false)
    }
  }

  const handleUploadCpa = async (scope: 'all' | 'selected') => {
    if (!isChatGPT) return

    const isSelectedScope = scope === 'selected'
    const ids = isSelectedScope ? selectedRowKeys.map((key) => Number(key)).filter((id) => Number.isInteger(id) && id > 0) : []

    if (isSelectedScope && ids.length === 0) {
      message.warning('请先选择要上传的账号')
      return
    }

    setCpaUploadingScope(scope)
    try {
      const payload = isSelectedScope
        ? { ids }
        : {
            status: filterStatus || undefined,
            email: search || undefined,
          }
      const result = await apiFetch('/accounts/chatgpt/upload-cpa', {
        method: 'POST',
        body: JSON.stringify(payload),
      })

      setCpaResult({
        title: isSelectedScope ? `上传选中的 ${result.total} 个账号到 CPA` : `整体上传 ${result.total} 个账号到 CPA`,
        total: result.total,
        success: result.success,
        failed: result.failed,
        items: result.items || [],
      })

      if (result.failed === 0) {
        message.success(`CPA 上传完成：成功 ${result.success} 个`)
      } else if (result.success === 0) {
        message.error(`CPA 上传失败：共 ${result.failed} 个`)
      } else {
        message.warning(`CPA 上传完成：成功 ${result.success} 个，失败 ${result.failed} 个`)
      }
    } catch (e: any) {
      let errorText = e?.message || '上传失败'
      try {
        const parsed = JSON.parse(errorText)
        errorText = parsed.detail || errorText
      } catch {
        // noop
      }
      message.error(`CPA 上传失败: ${errorText}`)
    } finally {
      setCpaUploadingScope(null)
    }
  }

  const handleRegister = async () => {
    const values = await registerForm.validateFields()
    setRegisterLoading(true)
    try {
      const cfg = await apiFetch('/config')
      const executorType = normalizeExecutorForPlatform(currentPlatform, cfg.default_executor)
      const res = await apiFetch('/tasks/register', {
        method: 'POST',
        body: JSON.stringify({
          platform: currentPlatform,
          count: values.count,
          concurrency: values.concurrency,
          register_delay_seconds: values.register_delay_seconds || 0,
          executor_type: executorType,
          captcha_solver: cfg.default_captcha_solver || 'yescaptcha',
          proxy: null,
          extra: {
            mail_provider: cfg.mail_provider || 'laoudo',
            laoudo_auth: cfg.laoudo_auth,
            laoudo_email: cfg.laoudo_email,
            laoudo_account_id: cfg.laoudo_account_id,
            yescaptcha_key: cfg.yescaptcha_key,
            moemail_api_url: cfg.moemail_api_url,
            duckmail_address: cfg.duckmail_address,
            duckmail_password: cfg.duckmail_password,
            duckmail_api_url: cfg.duckmail_api_url,
            duckmail_provider_url: cfg.duckmail_provider_url,
            duckmail_bearer: cfg.duckmail_bearer,
            freemail_api_url: cfg.freemail_api_url,
            freemail_admin_token: cfg.freemail_admin_token,
            freemail_username: cfg.freemail_username,
            freemail_password: cfg.freemail_password,
            cfworker_api_url: cfg.cfworker_api_url,
            cfworker_admin_token: cfg.cfworker_admin_token,
            cfworker_domain: cfg.cfworker_domain,
            cfworker_fingerprint: cfg.cfworker_fingerprint,
          },
        }),
      })
      setTaskId(res.task_id)
    } finally {
      setRegisterLoading(false)
    }
  }

  const handleDetailSave = async () => {
    const values = await detailForm.validateFields()
    await apiFetch(`/accounts/${currentAccount.id}`, {
      method: 'PATCH',
      body: JSON.stringify(values),
    })
    message.success('保存成功')
    setDetailModalOpen(false)
    load()
  }

  const columns: any[] = [
    {
      title: '邮箱',
      dataIndex: 'email',
      key: 'email',
      render: (text: string) => (
        <Text copyable={{ text }} style={{ fontFamily: 'monospace', fontSize: 12 }}>
          {text}
        </Text>
      ),
    },
    {
      title: '密码',
      dataIndex: 'password',
      key: 'password',
      render: (text: string) => (
        <Space>
          <Text style={{ fontFamily: 'monospace', fontSize: 12, filter: 'blur(4px)' }}>{text}</Text>
          <Button type="text" size="small" icon={<CopyOutlined />} onClick={() => copyText(text)} />
        </Space>
      ),
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => <Tag color={STATUS_COLORS[status] || 'default'}>{status}</Tag>,
    },
    {
      title: '地区',
      dataIndex: 'region',
      key: 'region',
      render: (text: string) => text || '-',
    },
    {
      title: '试用链接',
      dataIndex: 'cashier_url',
      key: 'cashier_url',
      render: (url: string) =>
        url ? (
          <Space>
            <Button type="text" size="small" icon={<CopyOutlined />} onClick={() => copyText(url)} />
            <Button type="text" size="small" icon={<LinkOutlined />} onClick={() => window.open(url, '_blank')} />
          </Space>
        ) : (
          '-'
        ),
    },
    {
      title: '注册时间',
      dataIndex: 'created_at',
      key: 'created_at',
      render: (text: string) => (text ? new Date(text).toLocaleDateString() : '-'),
    },
    {
      title: '操作',
      key: 'action',
      render: (_: any, record: any) => (
        <Space>
          <Button type="link" size="small" onClick={() => { setCurrentAccount(record); setDetailModalOpen(true); }}>
            详情
          </Button>
          <Popconfirm title="确认删除？" onConfirm={() => handleDelete(record.id)}>
            <Button type="link" size="small" danger>
              删除
            </Button>
          </Popconfirm>
          <ActionMenu acc={record} onRefresh={load} />
        </Space>
      ),
    },
  ]

  return (
    <div>
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', flexWrap: 'wrap', gap: 8 }}>
        <Space>
          <Input.Search
            placeholder="搜索邮箱..."
            allowClear
            onSearch={setSearch}
            style={{ width: 200 }}
          />
          <Select
            placeholder="状态筛选"
            allowClear
            style={{ width: 120 }}
            onChange={setFilterStatus}
            options={[
              { value: 'registered', label: '已注册' },
              { value: 'trial', label: '试用中' },
              { value: 'subscribed', label: '已订阅' },
              { value: 'expired', label: '已过期' },
              { value: 'invalid', label: '已失效' },
            ]}
          />
          {isChatGPT && (
            <Space size={4}>
              <Text type="secondary">401并发</Text>
              <InputNumber
                min={1}
                max={64}
                value={check401Workers}
                onChange={(value) => setCheck401Workers(Math.min(64, Math.max(1, Number(value || 8))))}
                style={{ width: 88 }}
                disabled={checking401Scope !== null}
              />
            </Space>
          )}
          <Text type="secondary">{total} 个账号</Text>
          {selectedRowKeys.length > 0 && (
            <Text type="success">已选 {selectedRowKeys.length} 个</Text>
          )}
        </Space>
        <Space>
          {total > 0 && (
            <Popconfirm
              title={
                search || filterStatus
                  ? `确认删除当前筛选的 ${total} 个 ${currentPlatform} 账号？`
                  : `确认删除当前页面全部 ${total} 个 ${currentPlatform} 账号？`
              }
              onConfirm={handleDeleteAll}
            >
              <Button danger icon={<DeleteOutlined />} loading={deleteAllLoading}>
                全部删除
              </Button>
            </Popconfirm>
          )}
          {selectedRowKeys.length > 0 && (
            <Popconfirm title={`确认删除选中的 ${selectedRowKeys.length} 个账号？`} onConfirm={handleBatchDelete}>
              <Button danger icon={<DeleteOutlined />} disabled={deleteAllLoading}>删除 {selectedRowKeys.length} 个</Button>
            </Popconfirm>
          )}
          {isChatGPT && selectedRowKeys.length > 0 && (
            <Popconfirm
              title={`确认检测选中的 ${selectedRowKeys.length} 个 ChatGPT 账号是否 401 无效？`}
              onConfirm={() => handleCheck401('selected')}
            >
              <Button
                loading={checking401Scope === 'selected'}
                disabled={checking401Scope === 'all'}
              >
                检测选中401
              </Button>
            </Popconfirm>
          )}
          {isChatGPT && (
            <Popconfirm
              title={
                search || filterStatus
                  ? `确认检测当前筛选的 ${total} 个 ChatGPT 账号是否 401 无效？`
                  : `确认整体检测全部 ${total} 个 ChatGPT 账号是否 401 无效？`
              }
              onConfirm={() => handleCheck401('all')}
              disabled={total === 0}
            >
              <Button
                loading={checking401Scope === 'all'}
                disabled={total === 0 || checking401Scope === 'selected'}
              >
                整体检测401
              </Button>
            </Popconfirm>
          )}
          {isChatGPT && (
            <Popconfirm
              title={
                selectedRowKeys.length > 0
                  ? `确认一键删除选中范围内已标记为 401 无效的账号？正常账号会自动跳过。`
                  : search || filterStatus
                    ? `确认一键删除当前筛选范围内已标记为 401 无效的账号？正常账号会自动跳过。`
                    : `确认一键删除全部已标记为 401 无效的 ChatGPT 账号？正常账号会自动跳过。`
              }
              onConfirm={handleDeleteInvalid401}
              disabled={total === 0}
            >
              <Button
                danger
                loading={deletingInvalid401}
                disabled={total === 0}
              >
                一键删除401无效
              </Button>
            </Popconfirm>
          )}
          {isChatGPT && selectedRowKeys.length > 0 && (
            <Popconfirm
              title={`确认上传选中的 ${selectedRowKeys.length} 个 ChatGPT 账号到 CPA？`}
              onConfirm={() => handleUploadCpa('selected')}
            >
              <Button
                icon={<UploadOutlined />}
                loading={cpaUploadingScope === 'selected'}
                disabled={cpaUploadingScope === 'all'}
              >
                上传选中到 CPA
              </Button>
            </Popconfirm>
          )}
          {isChatGPT && (
            <Popconfirm
              title={
                search || filterStatus
                  ? `确认上传当前筛选的 ${total} 个 ChatGPT 账号到 CPA？`
                  : `确认整体上传全部 ${total} 个 ChatGPT 账号到 CPA？`
              }
              onConfirm={() => handleUploadCpa('all')}
              disabled={total === 0}
            >
              <Button
                icon={<UploadOutlined />}
                loading={cpaUploadingScope === 'all'}
                disabled={total === 0 || cpaUploadingScope === 'selected'}
              >
                整体上传 CPA
              </Button>
            </Popconfirm>
          )}
          <Button icon={<UploadOutlined />} onClick={() => setImportModalOpen(true)}>导入</Button>
          <Button icon={<DownloadOutlined />} onClick={exportCsv} disabled={accounts.length === 0}>导出</Button>
          <Button icon={<PlusOutlined />} onClick={() => setAddModalOpen(true)}>新增</Button>
          <Button type="primary" icon={<PlusOutlined />} onClick={() => setRegisterModalOpen(true)}>注册</Button>
          <Button icon={<ReloadOutlined spin={loading} />} onClick={load} disabled={deleteAllLoading} />
        </Space>
      </div>

      <Table
        rowKey="id"
        columns={columns}
        dataSource={accounts}
        loading={loading}
        rowSelection={{
          selectedRowKeys,
          onChange: setSelectedRowKeys,
        }}
        pagination={{ pageSize: 20, showSizeChanger: false }}
        onRow={(record) => ({
          onDoubleClick: () => {
            setCurrentAccount(record)
            setDetailModalOpen(true)
          },
        })}
      />

      <Modal
        title={`注册 ${currentPlatform}`}
        open={registerModalOpen}
        onCancel={() => { setRegisterModalOpen(false); setTaskId(null); registerForm.resetFields(); }}
        footer={null}
        width={500}
        maskClosable={false}
      >
        {!taskId ? (
          <Form form={registerForm} layout="vertical" onFinish={handleRegister}>
            <Form.Item name="count" label="注册数量" initialValue={1} rules={[{ required: true }]}>
              <Input type="number" min={1} max={99} />
            </Form.Item>
            <Form.Item name="concurrency" label="并发数" initialValue={1} rules={[{ required: true }]}>
              <Input type="number" min={1} max={5} />
            </Form.Item>
            <Form.Item name="register_delay_seconds" label="每个注册延迟(秒)" initialValue={0}>
              <InputNumber min={0} precision={1} step={0.5} style={{ width: '100%' }} placeholder="0 = 不延迟" />
            </Form.Item>
            <Form.Item>
              <Button type="primary" htmlType="submit" block loading={registerLoading}>
                开始注册
              </Button>
            </Form.Item>
          </Form>
        ) : (
          <LogPanel taskId={taskId} onDone={() => { load(); }} />
        )}
      </Modal>

      <Modal
        title="手动新增账号"
        open={addModalOpen}
        onCancel={() => { setAddModalOpen(false); addForm.resetFields(); }}
        onOk={handleAdd}
        maskClosable={false}
      >
        <Form form={addForm} layout="vertical">
          <Form.Item name="email" label="邮箱" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="password" label="密码" rules={[{ required: true }]}>
            <Input.Password />
          </Form.Item>
          <Form.Item name="token" label="Token">
            <Input />
          </Form.Item>
          <Form.Item name="cashier_url" label="试用链接">
            <Input />
          </Form.Item>
          <Form.Item name="status" label="状态" initialValue="registered">
            <Select
              options={[
                { value: 'registered', label: '已注册' },
                { value: 'trial', label: '试用中' },
                { value: 'subscribed', label: '已订阅' },
              ]}
            />
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title="批量导入"
        open={importModalOpen}
        onCancel={() => { setImportModalOpen(false); setImportText(''); }}
        onOk={handleImport}
        confirmLoading={importLoading}
        maskClosable={false}
      >
        <p style={{ marginBottom: 8, fontSize: 12, color: '#7a8ba3' }}>
          每行格式: <code style={{ background: 'rgba(255,255,255,0.1)', padding: '2px 4px', borderRadius: 4 }}>email password [cashier_url]</code>
        </p>
        <Input.TextArea
          value={importText}
          onChange={(e) => setImportText(e.target.value)}
          rows={8}
          style={{ fontFamily: 'monospace' }}
        />
      </Modal>

      <Modal
        title="账号详情"
        open={detailModalOpen}
        onCancel={() => setDetailModalOpen(false)}
        onOk={handleDetailSave}
        maskClosable={false}
      >
        {currentAccount && (
          <Form form={detailForm} layout="vertical" initialValues={currentAccount}>
            <Form.Item name="status" label="状态">
              <Select
                options={[
                  { value: 'registered', label: '已注册' },
                  { value: 'trial', label: '试用中' },
                  { value: 'subscribed', label: '已订阅' },
                  { value: 'expired', label: '已过期' },
                  { value: 'invalid', label: '已失效' },
                ]}
              />
            </Form.Item>
            <Form.Item name="token" label="Token">
              <Input.TextArea rows={2} style={{ fontFamily: 'monospace' }} />
            </Form.Item>
          </Form>
        )}
      </Modal>

      <Modal
        title={check401Result?.title || '401 检测结果'}
        open={Boolean(check401Result)}
        onCancel={() => setCheck401Result(null)}
        onOk={() => setCheck401Result(null)}
        width={760}
        maskClosable={false}
      >
        {check401Result && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <div style={{ fontSize: 13, color: '#475569' }}>
              总计 {check401Result.total} 个，有效 {check401Result.valid} 个，401 无效 {check401Result.invalid401} 个，异常 {check401Result.failed} 个，并发 {check401Result.workers}
            </div>
            <div
              style={{
                maxHeight: 360,
                overflow: 'auto',
                padding: 12,
                border: '1px solid #e5e7eb',
                borderRadius: 8,
                background: '#fafafa',
                fontFamily: 'monospace',
                fontSize: 12,
                whiteSpace: 'pre-wrap',
              }}
            >
              {check401Result.items.map((item, index) => {
                const color = item.invalid_401 ? '#dc2626' : item.ok ? '#059669' : '#d97706'
                const prefix = item.invalid_401 ? '401' : item.ok ? '✓' : '!'
                const statusText = item.status_code ? ` [HTTP ${item.status_code}]` : ''
                return (
                  <div
                    key={`${item.id || item.email || 'row'}-${index}`}
                    style={{ color, lineHeight: 1.6 }}
                  >
                    {prefix} {item.email || `账号 ${item.id || '-'}`}{statusText} - {item.msg}
                  </div>
                )
              })}
            </div>
          </div>
        )}
      </Modal>

      <Modal
        title={cpaResult?.title || 'CPA 上传结果'}
        open={Boolean(cpaResult)}
        onCancel={() => setCpaResult(null)}
        onOk={() => setCpaResult(null)}
        width={760}
        maskClosable={false}
      >
        {cpaResult && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <div style={{ fontSize: 13, color: '#475569' }}>
              总计 {cpaResult.total} 个，成功 {cpaResult.success} 个，失败 {cpaResult.failed} 个
            </div>
            <div
              style={{
                maxHeight: 360,
                overflow: 'auto',
                padding: 12,
                border: '1px solid #e5e7eb',
                borderRadius: 8,
                background: '#fafafa',
                fontFamily: 'monospace',
                fontSize: 12,
                whiteSpace: 'pre-wrap',
              }}
            >
              {cpaResult.items.map((item, index) => (
                <div
                  key={`${item.id || item.email || 'row'}-${index}`}
                  style={{ color: item.ok ? '#059669' : '#dc2626', lineHeight: 1.6 }}
                >
                  {item.ok ? '✓' : '✗'} {item.email || `账号 ${item.id || '-'}`} - {item.msg}
                </div>
              ))}
            </div>
          </div>
        )}
      </Modal>
    </div>
  )
}
